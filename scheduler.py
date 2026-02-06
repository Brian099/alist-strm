import os
import uuid
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# 导入具体的任务函数
from main import main as run_strm_creation
from strm_validator import run_validator as run_strm_validation

# 配置日志
logger = logging.getLogger('scheduler')

def convert_to_cron_time(interval_type, interval_value):
    interval_value = int(interval_value)
    if interval_type == 'minute':
        if not 1 <= interval_value <= 59:
            raise ValueError('分钟间隔值必须在 1 到 59 之间')
        return f'*/{interval_value} * * * *'
    elif interval_type == 'hourly':
        if not 1 <= interval_value <= 23:
            raise ValueError('小时间隔值必须在 1 到 23 之间')
        return f'0 */{interval_value} * * *'
    elif interval_type == 'daily':
        if not 1 <= interval_value <= 31:
            raise ValueError('天数间隔值必须在 1 到 31 之间')
        return f'0 0 */{interval_value} * *'
    elif interval_type == 'weekly':
        if not 0 <= interval_value <= 6:
            raise ValueError('星期值必须在 0（周日）到 6（周六）之间')
        return f'0 0 * * {interval_value}'
    elif interval_type == 'monthly':
        if not 1 <= interval_value <= 12:
            raise ValueError('月份间隔值必须在 1 到 12 之间')
        return f'0 0 1 */{interval_value} *'
    else:
        raise ValueError('不支持的间隔类型')

def parse_cron_time(cron_time):
    cron_parts = cron_time.split()
    if len(cron_parts) != 5:
        return 'custom', '', '自定义时间'

    minute, hour, day, month, weekday = cron_parts

    if minute.startswith('*/') and hour == '*' and day == '*' and month == '*' and weekday == '*':
        interval_value = minute[2:]
        interval_type = 'minute'
        description = f"每 {interval_value} 分钟"
        return interval_type, interval_value, description
    elif minute == '0' and hour.startswith('*/') and day == '*' and month == '*' and weekday == '*':
        interval_value = hour[2:]
        interval_type = 'hourly'
        description = f"每 {interval_value} 小时"
        return interval_type, interval_value, description
    elif minute == '0' and hour == '0' and day.startswith('*/') and month == '*' and weekday == '*':
        interval_value = day[2:]
        interval_type = 'daily'
        description = f"每 {interval_value} 天"
        return interval_type, interval_value, description
    elif minute == '0' and hour == '0' and day == '*' and month == '*' and weekday != '*':
        interval_value = weekday
        interval_type = 'weekly'
        weekdays = {
            '0': '星期日',
            '1': '星期一',
            '2': '星期二',
            '3': '星期三',
            '4': '星期四',
            '5': '星期五',
            '6': '星期六'
        }
        weekday_name = weekdays.get(interval_value, interval_value)
        description = f"每周的 {weekday_name}"
        return interval_type, interval_value, description
    elif minute == '0' and hour == '0' and day == '1' and month.startswith('*/') and weekday == '*':
        interval_value = month[2:]
        interval_type = 'monthly'
        description = f"每 {interval_value} 个月"
        return interval_type, interval_value, description
    else:
        return 'custom', '', '自定义时间'

class SchedulerManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SchedulerManager, cls).__new__(cls)
            cls._instance.scheduler = None
        return cls._instance

    def init_scheduler(self, db_url='sqlite:////config/jobs.db'):
        if self.scheduler:
            return

        # 配置 JobStore
        jobstores = {
            'default': SQLAlchemyJobStore(url=db_url)
        }
        
        # 配置 Executors
        executors = {
            'default': ThreadPoolExecutor(20)
        }
        
        # 配置 JobDefaults
        job_defaults = {
            'coalesce': False,
            'max_instances': 3
        }

        self.scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
        self.scheduler.start()
        logger.info("APScheduler initialized and started.")

    def add_task(self, task_name, cron_expression, config_id, task_mode, is_enabled=True):
        """
        添加一个新的定时任务
        """
        task_id = str(uuid.uuid4())
        trigger = CronTrigger.from_crontab(cron_expression)
        
        func = None
        args = []
        
        if task_mode == 'strm_creation':
            func = run_strm_creation
            args = [config_id, task_id]
        elif task_mode == 'strm_validation_quick':
            func = run_strm_validation
            args = [config_id, 'quick', task_id]
        elif task_mode == 'strm_validation_slow':
            func = run_strm_validation
            args = [config_id, 'slow', task_id]
        else:
            raise ValueError(f"Unknown task mode: {task_mode}")

        # 存储元数据以便后续检索
        metadata = {
            'cron_time': cron_expression,
            'task_mode': task_mode,
            'config_id': config_id,
            'task_name': task_name
        }

        job = self.scheduler.add_job(
            func=func,
            trigger=trigger,
            args=args,
            kwargs={'metadata': metadata},
            id=task_id,
            name=task_name,
            replace_existing=True,
            next_run_time=None if not is_enabled else None
        )
        
        if not is_enabled:
            job.pause()
            
        return task_id

    def update_task(self, task_id, cron_expression=None, config_id=None, task_mode=None, task_name=None, is_enabled=None):
        job = self.scheduler.get_job(task_id)
        if not job:
            raise ValueError(f"Task with ID {task_id} not found")

        # 获取现有的 metadata
        metadata = job.kwargs.get('metadata', {})
        
        changes = {}
        if cron_expression:
            trigger = CronTrigger.from_crontab(cron_expression)
            changes['trigger'] = trigger
            metadata['cron_time'] = cron_expression
            
        if task_name:
            changes['name'] = task_name
            metadata['task_name'] = task_name
            
        if config_id is not None:
            metadata['config_id'] = config_id
            # 如果 args 包含 config_id，也需要更新 args
            # 假设 args[0] 是 config_id
            args = list(job.args)
            if len(args) > 0:
                args[0] = config_id
                changes['args'] = args

        if task_mode:
            metadata['task_mode'] = task_mode
            if task_mode == 'strm_creation':
                changes['func'] = run_strm_creation
            elif task_mode == 'strm_validation_quick':
                changes['func'] = run_strm_validation
                # 可能需要更新 args，这里简化处理，假设 task_mode 变更不频繁或需要更细致处理
            elif task_mode == 'strm_validation_slow':
                changes['func'] = run_strm_validation
            
        if is_enabled is not None:
            if is_enabled:
                job.resume()
            else:
                job.pause()

        # 更新 kwargs 中的 metadata
        changes['kwargs'] = {'metadata': metadata}
        
        job.modify(**changes)
        return True

    def delete_task(self, task_id):
        try:
            self.scheduler.remove_job(task_id)
        except Exception as e:
            logger.error(f"Error deleting task {task_id}: {e}")
            # 即使出错也认为成功（可能任务已不存在）
            pass

    def list_tasks(self):
        tasks = []
        if not self.scheduler:
            return tasks
            
        jobs = self.scheduler.get_jobs()
        for job in jobs:
            metadata = job.kwargs.get('metadata', {})
            cron_time = metadata.get('cron_time', '')
            interval_type, interval_value, description = parse_cron_time(cron_time)
            
            task_info = {
                'task_id': job.id,
                'task_name': job.name,
                'next_run_time': job.next_run_time,
                'is_enabled': job.next_run_time is not None,
                'config_id': metadata.get('config_id'),
                'task_mode': metadata.get('task_mode'),
                'cron_time': cron_time,
                'interval_type': interval_type,
                'interval_value': interval_value,
                'interval_description': description
            }
            tasks.append(task_info)
        return tasks

    def run_task_now(self, task_id):
        job = self.scheduler.get_job(task_id)
        if not job:
            raise ValueError(f"Task with ID {task_id} not found")
        
        # 创建一个一次性任务立即执行
        run_id = f"{task_id}_manual_{uuid.uuid4().hex[:8]}"
        self.scheduler.add_job(
            func=job.func,
            args=job.args,
            kwargs=job.kwargs,
            id=run_id,
            name=f"{job.name} (Manual Run)",
            misfire_grace_time=3600,
            replace_existing=False
        )
        logger.info(f"Triggered manual run for task {task_id} as {run_id}")
        return run_id

    def stop_task(self, task_id):
        # 目前使用 ThreadPoolExecutor，无法直接强制停止正在运行的线程。
        # 如果需要此功能，需要在任务函数中支持停止标志，或者改用 ProcessPoolExecutor。
        # 这里暂时只记录日志。
        logger.warning(f"Stop task requested for {task_id}, but thread termination is not supported yet.")
        return False, "当前运行模式不支持强制停止任务，请等待任务完成。"

scheduler_manager = SchedulerManager()
