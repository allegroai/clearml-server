from logging import Logger
from uuid import uuid4

from apiserver.bll.queue import QueueBLL
from apiserver.config_repo import config
from apiserver.database.model.company import Company
from apiserver.database.model.queue import Queue
from apiserver.database.model.settings import Settings, SettingKeys

log = config.logger(__file__)


def _ensure_company(company_id, company_name, log: Logger):
    company = Company.objects(id=company_id).only("id").first()
    if company:
        return company_id

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
    Settings.add_value(SettingKeys.server__uuid, str(uuid4()))
