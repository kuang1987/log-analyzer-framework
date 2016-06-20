#!/usr/bin/env python

from pytz import timezone,utc

def utc2local(dt):
    utc_dt = utc.localize(dt)
    tz = timezone('Asia/Shanghai')
    local_dt = tz.normalize(utc_dt.astimezone(tz))
    return local_dt

def dt2str(dt,fmt):
    return dt.strftime(fmt)
