import requests
import hashlib
from bs4 import BeautifulSoup
import re
from datetime import datetime
import csv
from io import StringIO
import os
import json
from libs.logs import logger
from typing import List, Dict, Optional, Any
from urllib.parse import urlencode

class ParseVendors:
    """Class for parsing vendor data from Bitrix24."""
    
    CONFIG = {
        'base_url': 'https://vendors.bitrix24.ru',
        'endpoints': {
            'auth': '/auth/',
            'daily_payments': '/sale/payout.php',
            'app_list': '/app/',
            'app_client_list': '/sale/clients.php',
            'filter': '/bitrix/services/main/ajax.php'
        },
        'headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'Accept-Language': 'ru,en-US',
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Bitrix-Site-Id': 'mv'
        },
        'ajax_ids': {
            'payout': '08121894e876869a8373dc61fb5e7f3e',
            'app_list': '2eac8fa73aa98e2f7e412d509f9fe12b'
        },
        'max_pages': 100  # Prevent infinite pagination
    }

    def __init__(self, date_time: Optional[datetime] = None):
        """Initialize the ParseVendors class.

        Args:
            date_time: Optional datetime object for filtering data. Defaults to current time.
        """
        self.date_time = date_time or datetime.now()
        self.session = requests.Session()
        self.session.headers.update(self.CONFIG['headers'])
        self.session_id: Optional[str] = None
        self.auth()

    def _flatten_dict(self, d, parent_key='', sep='['):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}]" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def auth(self) -> None:
        """Authenticate with the Bitrix24 vendor site."""
        try:
            response = self.session.post(self.CONFIG['base_url'] + self.CONFIG['endpoints']['auth'], data={
                'AUTH_FORM': 'Y',
                'TYPE': 'AUTH',
                'USER_LOGIN': os.environ.get('VENDORS_LOGIN'),
                'USER_PASSWORD': os.environ.get('VENDORS_PASSWORD'),
            })
            response.raise_for_status()
            match = re.search(r'"bitrix_sessid":"([a-f0-9]+)"', response.text)
            if match:
                self.session_id = match.group(1)
                logger.info("Authenticated successfully with session ID: %s", self.session_id)
            else:
                raise ValueError("Session ID not found in response")
        except requests.RequestException as e:
            logger.error("Authentication failed: %s", e)
            raise Exception(f"Authentication error: {e}")

    def _send_post_request(self, url: str, params: Dict[str, Any], data: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> requests.Response:
        """Helper method to send POST requests with error handling.

        Args:
            url: The URL to send the request to.
            params: Query parameters.
            data: Form data.
            headers: Additional headers.

        Returns:
            Response object.

        Raises:
            requests.HTTPError: If the response status code is not 200.
        """

        try:
            response = self.session.post(url, params=params, data=data, headers=headers)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error("POST request failed for %s: %s", url, e)
            raise

    def set_filter(self, filter_id: str, grid_id: str) -> None:
        """Set a filter for data retrieval.

        Args:
            filter_id: Filter ID for the request.
            grid_id: Grid ID for the request.
        """
        preset_id = 'tmp_filter'
        filter_data = {
            'params': {
                'FILTER_ID': filter_id,
                'GRID_ID': grid_id,
                'action': 'setFilter',
                'forAll': False,
                'commonPresetsId': '',
                'apply_filter': 'Y',
                'clear_filter': 'N',
                'with_preset': 'N',
                'save': 'Y',
                'isSetOutside': 'false',
            },
            'data': {
                'fields': {
                    'FIND': '',
                    'DATE_OF_USE_from': '',
                    'DATE_OF_USE_to': '',
                    'DATE_OF_USE_days': '',
                    'DATE_OF_USE_quarter': '',
                    'DATE_OF_USE_datesel': 'MONTH',
                    'DATE_OF_USE_month': self.date_time.strftime('%-m'),
                    'DATE_OF_USE_year': self.date_time.strftime('%Y')
                },
                'rows': 'DATE_OF_USE',
                'preset_id': preset_id,
                'name': 'Фильтр'
            }
        }
        filter_data = self._flatten_dict(filter_data)

        f = self._send_post_request(
            self.CONFIG['base_url'] + self.CONFIG['endpoints']['filter'],
            params={'mode': 'ajax', 'c': 'bitrix:main.ui.filter', 'action': 'setFilter'},
            data=filter_data,
            headers={'X-Bitrix-Csrf-Token': self.session_id}
        )

        apply_filter_data = {'apply_filter': 'Y', 'clear_nav': 'Y'}
        r = self._send_post_request(
            self.CONFIG['base_url'] + self.CONFIG['endpoints']['daily_payments'],
            params={
                'sessid': self.session_id,
                'internal': 'true',
                'grid_id': grid_id,
                'apply_filter': 'Y',
                'clear_nav': 'Y',
                'grid_action': 'showpage',
                'bxajaxid': self.CONFIG['ajax_ids']['payout'],
            },
            data=apply_filter_data
        )

    def _parse_csv_payments(self, content: str, is_premium: bool = False) -> List[Dict[str, Any]]:
        """Parse CSV content into a list of payment dictionaries.

        Args:
            content: CSV content as a string.
            is_premium: Whether to process premium payments (adds hash and DATE_PARSE).

        Returns:
            List of payment dictionaries.
        """
        payments = []
        try:
            reader = csv.reader(StringIO(content), delimiter=';')
            header = next(reader)[:-1]  # Remove last empty element
            for row in reader:
                if len(row) <= 1:
                    continue
                payment = {}
                for key, value in zip(header, row):
                    try:
                        if key in ('AMOUNT', 'ALL_AMOUNT'):
                            value = float(value.replace(' ', '').replace(',', '.'))
                        elif key in ('DATE_OF_USE', 'SUBSCRIPTION_START', 'SUBSCRIPTION_END'):
                            value = datetime.strptime(value, '%d.%m.%Y').strftime('%Y-%m-%d')
                        payment[key] = value
                    except (ValueError, TypeError) as e:
                        logger.warning("Error processing field %s with value %s: %s", key, value, e)
                        payment[key] = value  # Keep raw value if parsing fails
                if is_premium:
                    payment['hash'] = hashlib.sha256((payment['MEMBER_ID'] + self.date_time.strftime('01-%m-%Y')).encode()).hexdigest()
                    payment['DATE_PARSE'] = self.date_time.strftime('%Y-%m-01')
                    payments.append(payment)
                elif not is_premium and payment.get('ID'):
                    payments.append(payment)
        except csv.Error as e:
            logger.error("CSV parsing error: %s", e)
            raise
        return payments

    def get_payments(self) -> List[Dict[str, Any]]:
        """Fetch standard payment data.

        Returns:
            List of payment dictionaries.
        """
        self.set_filter('mp24_subscription_partner', 'mp24_subscription_partner')
        try:
            response = self.session.get(
                self.CONFIG['base_url'] + self.CONFIG['endpoints']['daily_payments'],
                params={
                    'export': 'Y',
                    'sessid': self.session_id,
                    'type': 'payouts',
                }
            )
            response.raise_for_status()
            return self._parse_csv_payments(response.text)
        except requests.RequestException as e:
            logger.error("Failed to fetch payments: %s", e)
            raise

    def get_premium_payments(self) -> List[Dict[str, Any]]:
        """Fetch premium payment data.

        Returns:
            List of premium payment dictionaries.
        """
        self.set_filter('mp24_subscription_premium_for_partners', 'mp24_subscription_premium_for_partners')
        try:
            response = self.session.get(
                self.CONFIG['base_url'] + self.CONFIG['endpoints']['daily_payments'],
                params={
                    'export': 'Y',
                    'sessid': self.session_id,
                    'type': 'payouts_premium',
                }
            )
            response.raise_for_status()
            return self._parse_csv_payments(response.text, is_premium=True)
        except requests.RequestException as e:
            logger.error("Failed to fetch premium payments: %s", e)
            raise

    def get_app_list(self) -> List[Dict[str, Any]]:
        """Fetch the list of applications.

        Returns:
            List of application dictionaries.
        """
        result = []
        for page in range(1, self.CONFIG['max_pages'] + 1):
            try:
                response = self.session.get(
                    self.CONFIG['base_url'] + self.CONFIG['endpoints']['app_list'],
                    params={
                        'internal': 'true',
                        'sessid': self.session_id,
                        'grid_id': 'vendor_app_list',
                        'grid_action': 'pagination',
                        'nav-moderator-app-list': f'page-{page}',
                        'bxajaxid': self.CONFIG['ajax_ids']['app_list']
                    }
                )
                response.raise_for_status()
                # Using html.parser as per user preference; consider 'lxml' for better performance
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', id='vendor_app_list_table')
                if not table:
                    break
                headers = [th.text.strip() for th in table.find('thead').find_all('th') if th.text.strip()]
                if not headers:
                    break
                rows = table.find_all('tr', class_='main-grid-row main-grid-row-body')
                if not rows:
                    break
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) < len(headers):
                        continue
                    row_data = {headers[i]: cells[i].text.strip() for i in range(len(headers))}
                    action_span = row.find('span', class_='main-grid-row-action-button')
                    if action_span and 'data-actions' in action_span.attrs:
                        actions = action_span['data-actions']
                        match = re.search(r"bx24vendorClients\((\d+)\)", actions)
                        if match:
                            row_data['id'] = match.group(1)
                    result.append(row_data)
            except requests.RequestException as e:
                logger.error("Failed to fetch app list page %d: %s", page, e)
                break
        return result

    def get_client_list(self, app_id: str) -> List[Dict[str, Any]]:
        """Fetch the list of clients for a given app ID.

        Args:
            app_id: The ID of the application.

        Returns:
            List of client dictionaries.
        """
        result = []
        for page in range(1, self.CONFIG['max_pages'] + 1):
            try:
                response = self.session.get(
                    self.CONFIG['base_url'] + self.CONFIG['endpoints']['app_client_list'],
                    params={
                        'ID': app_id,
                        'nav-client': f'page-{page}',
                    }
                )
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', id='mp24_client')
                if not table:
                    break
                header_row = table.find('tr', class_='bx-grid-head')
                if not header_row:
                    break
                headers = [td.text.strip() or 'actions' for td in header_row.find_all('td')]
                rows = table.find_all('tr', class_=lambda x: x in [None, ' dru'])
                if len(rows) <= 1:
                    break
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= len(headers):
                        row_data = {headers[i]: cells[i].text.strip() for i in range(len(headers))}
                        result.append(row_data)
            except requests.RequestException as e:
                logger.error("Failed to fetch client list page %d for app %s: %s", page, app_id, e)
                break
        return result
