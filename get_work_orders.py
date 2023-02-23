# flake8: noqa: E501

import requests
import os
from collections import defaultdict
import sys
import logging

TOKEN = os.getenv('DEEPO_TOKEN_DEV4')
if TOKEN is None:
    sys.exit(1)

# ORG_SLUG = 'telecom-sur-etagere'
# BASE_URL = 'studio.deepomatic.com'
# SITE_ID = 'b31aa789-96ef-4c05-8740-41b3cd5e1d73'
BASE_URL = 'web.vesta.dev4.k8s-stag.deepomatic.com'
ORG_SLUG = 'telecom-sur-etagere-for-tests'
SITE_ID = 'ed8cf35c-6325-4d38-86da-e8fb21c3d792'

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def append_slash(string: str) -> str:
    if not string.endswith('/'):
        return string + '/'
    return string

class DeepomaticClient:
    # In memory caching mechanism, it doesn't implement anything related to
    # Etag, cache invalidation, etc
    # I know I could have used functools.lru_cache here too, but this is more
    # explicit
    # This is just for demo purposes
    CACHE = defaultdict(dict)

    def __init__(self, api_key: str):
        logger.info('Initializing Client')
        self.session = requests.session()
        self.session.headers['Authorization'] = f'Token {api_key}'

    def app_version(self, id: str):
        logger.info('Fetching app version %s', id)
        if self.CACHE['app_version'].get(id):
            logger.debug('Cache hit for app version %s', id)
            return self.CACHE['app_version'][id]

        app_version = self.session.get(append_slash((
            f'https://{BASE_URL}/api/fs-app/v1/on-site/orgs/'
            f'{ORG_SLUG}/sites/{SITE_ID}/versions/{id}'
        ))).json()
        self.CACHE['app_version'][id] = app_version
        return app_version

    def task_groups(self, app_version_id: str, tgid: str = ''):
        logger.info('Fetching task groups for app version %s, tg %s', app_version_id, tgid)
        if self.CACHE['task_groups'].get(f'{app_version_id}/{tgid}'):
            logger.debug('Cache hit for task groups for app version %s and tg %s', app_version_id, tgid)
            return self.CACHE['task_groups'][f'{app_version_id}/{tgid}']

        task_groups = self.session.get(append_slash((
            f'https://{BASE_URL}/api/fs-app/v1/on-site/orgs/'
            f'{ORG_SLUG}/sites/{SITE_ID}/versions/'
            f'{app_version_id}/task-groups/{tgid}'
        ))).json()
        self.CACHE['task_groups'][f'{app_version_id}/{tgid}'] = task_groups
        return task_groups

    def work_order_types(self, app_version_id: str, wot_id: str = ''):
        logger.info('Fetching work_order_types for app_version_id %s, wot id %s', app_version_id, wot_id)
        if self.CACHE['work_order_types'].get(f'{app_version_id}/{wot_id}'):
            logger.debug('Cache hit for wot %s/%s', app_version_id, wot_id)
            return self.CACHE['work_order_types'][f'{app_version_id}/{wot_id}']
        if self.CACHE['work_order_types'].get(f'{app_version_id}/') and wot_id:
            logger.debug('Cache hit for wot listing')
            for wot in self.CACHE['work_order_types'][f'{app_version_id}/']['results']:
                if wot['id'] == wot_id:
                    return wot
            else:
                raise IndexError((
                    f'Cannot find work_order_type id {wot_id} '
                    f'for app version {app_version_id}'
                ))

        wots = self.session.get(append_slash((
            f'https://{BASE_URL}/api/fs-app/v1/on-site/orgs/'
            f'{ORG_SLUG}/sites/{SITE_ID}/versions/'
            f'{app_version_id}/work-order-types/'
        ))).json()
        for wot in wots['results']:
            self.CACHE['work_order_types'][f'{app_version_id}/{wot["id"]}'] = wot
        self.CACHE['work_order_types'][f'{app_version_id}/'] = wots
        return wot

    def analyses(self, work_order_id: str, task_group_id: str):
        logger.info('Fetching analyses for work_order %s and task_group %s', work_order_id, task_group_id)
        if self.CACHE['analyses'].get(f'{work_order_id}/{task_group_id}/analyses'):
            logger.debug('Cache hit for analyses for work_order %s and task_group %s', work_order_id, task_group_id)
            return self.CACHE['analyses'][f'{work_order_id}/{task_group_id}/analyses']

        analyses = self.session.get(append_slash((
            f'https://{BASE_URL}/api/fs-app/v1/on-site/orgs/'
            f'{ORG_SLUG}/sites/{SITE_ID}/work-orders/'
            f'{work_order_id}/task-groups/{task_group_id}/analyses'
        ))).json()
        if not analyses:
            analyses = [{}]
        self.CACHE['analyses'][f'{work_order_id}/{task_group_id}/analyses'] = analyses
        return analyses

    def work_orders(self):
        work_orders = self.session.get(append_slash((
            f'https://{BASE_URL}/api/fs-app/v1/on-site/orgs/'
            f'{ORG_SLUG}/sites/{SITE_ID}/work-orders/'
        ))).json()
        for work_order in work_orders['results']:
            app_version_id = work_order['app_version_id']
            all_task_groups = [
                self.task_groups(app_version_id, tg['id'])
                for tg in self.task_groups(app_version_id)
            ]
            wo_task_groups = [
                self.task_groups(app_version_id, tgid)
                for type_id in work_order.get('types', [])
                for tgid in self.work_order_types(
                    app_version_id, type_id
                )['task_groups']
            ]
            if not wo_task_groups:
                wo_task_groups = all_task_groups
            work_order['task_groups'] = wo_task_groups
            wo_tasks = [
                task
                for tg in wo_task_groups
                for task in tg['tasks']
            ]
            analyses = {
                task_group['id']: self.analyses(work_order['id'], task_group['id'])[0]
                for task_group in wo_task_groups
            }
            outcomes = {
                outcome['task_id']: outcome
                for analysis in analyses.values()
                for outcome in analysis.get('outcomes', [])
            }
            for task in wo_tasks:
                task['state'] = outcomes.get(task['id'])
            work_order['tasks'] = wo_tasks
            yield work_order


client = DeepomaticClient(TOKEN)

if __name__ == '__main__':
    import pdb
    pdb.set_trace()
