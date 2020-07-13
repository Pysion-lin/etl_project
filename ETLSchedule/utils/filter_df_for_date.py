import datetime
from ETLSchedule.settings.dev import Update_DATE


def filter_df(condition,df):
    now_day = datetime.datetime.now().strftime("%Y-%m-%d")
    now_date = datetime.datetime.now().strftime("%Y-%m-%d")
    forward_date = datetime.datetime.strptime(now_day, "%Y-%m-%d") - datetime.timedelta(days=Update_DATE)
    new_df = df.query("%s <= %r" % (condition,now_date)).query(
        "%s >= %r" % (condition,forward_date))  # 只更前几天
    return new_df

