def k_or_m():
    pass


def approx_value(value, currency):
    n = len(str(int(value)))
    d = 0
    r = 2
    suf = ''
    if n > 3:
        suf = 'k'
        d = 3
        r = 1
        if n > 6:
            suf = 'M'
            d = 6
            if n > 9:
                suf = 'B'
                d = 9
                if n > 12:
                    r = 0
    v = currency + format(round(value / (10 ** d), r), ',').replace(',', ' ') + suf
    return v



def split_digits(value):
    return format(value, ',').replace(',', ' ')


def make_pct(value, decimals, sign=False):
    s = ''
    if sign:
        if value < 0:
            s = '-'
        else:
            s = '+'
    return s + str(round(value * 100, decimals)) + '%'
