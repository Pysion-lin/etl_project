import datetime
from settings.dev import Update_DATE


def filter_df(condition,df):
    now_day = datetime.datetime.now().strftime("%Y-%m-%d")
    # now_date = datetime.datetime.now().strftime("%Y-%m-%d")
    forward_date = datetime.datetime.strptime(now_day, "%Y-%m-%d") - datetime.timedelta(days=Update_DATE)
    forward_date = forward_date.strftime("%Y-%m-%d")
    # new_df = df.query("%s <= %r" % (condition,now_date)).query(
    #     "%s >= %r" % (condition,forward_date))  # 只更前几天
    new_df = df.query("%s >= %r" % (condition,forward_date))
    return new_df


if __name__ == '__main__':
    import pandas as pd

    now_day = datetime.datetime.now().strftime("%Y-%m-%d")
    forward_date = datetime.datetime.strptime(now_day, "%Y-%m-%d")
    setp_date = datetime.timedelta(days=1)
    a = forward_date - setp_date
    b = forward_date + setp_date
    df = pd.DataFrame(data={"CREATE_TIME":[now_day,a,b]})
    filter_df("CREATE_TIME",df)