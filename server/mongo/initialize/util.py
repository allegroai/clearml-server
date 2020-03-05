from logging import Logger
from uuid import uuid4

from bll.queue import QueueBLL
from config import config
from config.info import get_default_company
from database.model.company import Company
from database.model.queue import Queue
from database.model.settings import Settings

log = config.logger(__file__)


def _ensure_company(log: Logger):
    company_id = get_default_company()
    company = Company.objects(id=company_id).only("id").first()
    if company:
        return company_id

    company_name = "trains"
    log.info(f"Creating company: {company_name}")
    company = Company(id=company_id, name=company_name)
    company.save()
    return company_id


def _ensure_default_queue(company):
    """
    If no queue is present for the company then
    create a new one and mark it as a default
    """
    queue = Queue.objects(company=company).only("id").first()
    if queue:
        return

    QueueBLL.create(company, name="default", system_tags=["default"])


def _ensure_uuid():
    Settings.add_value("server.uuid", str(uuid4()))
