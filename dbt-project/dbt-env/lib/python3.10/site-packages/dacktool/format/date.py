import datetime
from time import strptime


def yyyymmdd(d, separator=''):
    """
    :param d: datetime instance
    separator: string
    :return: string: yyyymmdd
    """
    month = d.month
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    day = d.day
    if day < 10:
        day = '0' + str(day)
    else:
        day = str(day)
    return separator.join([str(d.year), month, day])


def epoch_to_datetime(epoch):
    return datetime.datetime.fromtimestamp(epoch).strftime('%c')


def date_string_to_epoch(date):
    return int(strptime(date, "%Y-%m-%d").strftime("%s"))


def end_of_month(date):
    if date.month == 12:
        next_month = 1
        next_year = date.year + 1
    else:
        next_month = date.month + 1
        next_year = date.year
    next_date = datetime.datetime(next_year, next_month, 1)
    end_date = next_date + datetime.timedelta(days=-1)
    return str(end_date)[:10]


def start_of_month(date):
    date = datetime.datetime(date.year, date.month, 1)
    return str(date)[:10]


def n_months(date, n=12):
    '''
    :param date: datetime instance
    :param n: number of months
    :return: list of strings with min and max date of last n months
    '''
    all_month = [date.start_of_month(date) + " " + str(date)[:10]]
    i = 1
    new_date = date + datetime.timedelta(days=-27)
    while i != n:
        if new_date.month != date.month:
            all_month = [start_of_month(new_date) + " " + end_of_month(new_date)] + all_month
            new_date = new_date + datetime.timedelta(days=-27)
            i = i + 1
        else:
            new_date = new_date + datetime.timedelta(days=-27)
    return all_month


def date_pretty(date):
    return date.strftime("%A, %d %B %Y")
