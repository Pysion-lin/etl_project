from ETLSchedule.models.models import TaskScheduleModel
from ETLSchedule.models import session
import traceback


def update_task_schedule_status():
    try:
        task_schedulers = session.query(TaskScheduleModel).all()
        for scheduler in task_schedulers:
            scheduler.status = 0
        session.commit()
    except Exception as e:
        traceback.print_exc()



