from datetime import datetime
from libs.logs import logger
from libs.ydb import driver, upset_data
from libs.parser import ParseVendors

chunk_size = 100

def handler(event, context):
    try:
        date_time = datetime.strptime(event, '%d.%m.%Y') if event else None
    except:
        date_time = None
        
    parser = ParseVendors(date_time)

    premium_payments = parser.get_premium_payments()
    logger.info(f"Обновляем данные: payments_premium, {len(premium_payments)} записей")
    for i in range(0, len(premium_payments), chunk_size):
        chunk = premium_payments[i:i + chunk_size]
        upset_data('payments_premium', chunk)

    payments = parser.get_payments()
    logger.info(f"Обновляем данные: payments, {len(payments)} записей")
    for i in range(0, len(payments), chunk_size):
        chunk = payments[i:i + chunk_size]
        upset_data('payments', chunk)
        
    return {
        'statusCode': 200,
        'body': 'ok',
    }
