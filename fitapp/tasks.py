from datetime import timedelta
import logging
import sys

from celery import shared_task
from celery.exceptions import Ignore, Reject
from dateutil import parser
from django.core.cache import cache
from django.utils.timezone import utc
from fitbit.exceptions import HTTPBadRequest, HTTPTooManyRequests

from . import utils
from .models import UserFitbit, TimeSeriesData, TimeSeriesDataType


logger = logging.getLogger(__name__)
LOCK_EXPIRE = 60 * 5  # Lock expires in 5 minutes


@shared_task
def subscribe(fitbit_user, subscriber_id):
    """ Subscribe to the user's fitbit data """

    fbusers = UserFitbit.objects.filter(fitbit_user=fitbit_user)
    for fbuser in fbusers:
        fb = utils.create_fitbit(**fbuser.get_user_data())
        try:
            fb.subscription(fbuser.user.id, subscriber_id)
        except:
            exc = sys.exc_info()[1]
            logger.exception("Error subscribing user: %s" % exc)
            raise Reject(exc, requeue=False)


@shared_task
def unsubscribe(*args, **kwargs):
    """ Unsubscribe from a user's fitbit data """

    fb = utils.create_fitbit(**kwargs)
    try:
        for sub in fb.list_subscriptions()['apiSubscriptions']:
            if sub['ownerId'] == kwargs['user_id']:
                fb.subscription(sub['subscriptionId'], sub['subscriberId'],
                                method="DELETE")
    except:
        exc = sys.exc_info()[1]
        logger.exception("Error unsubscribing user: %s" % exc)
        raise Reject(exc, requeue=False)



@shared_task
def get_time_series_data(fitbit_user, cat, resource, date=None):
    """ Get the user's time series data, saved in UTC """

    try:
        _type = TimeSeriesDataType.objects.get(category=cat, resource=resource)
    except TimeSeriesDataType.DoesNotExist:
        logger.exception("The resource %s in category %s doesn't exist" % (
            resource, cat))
        raise Reject(sys.exc_info()[1], requeue=False)

    # Create a lock so we don't try to run the same task multiple times
    sdat = date.strftime('%Y-%m-%d') if date else 'ALL'
    lock_id = '{0}-lock-{1}-{2}-{3}'.format(__name__, fitbit_user, _type, sdat)
    if not cache.add(lock_id, 'true', LOCK_EXPIRE):
        logger.debug('Already retrieving %s data for date %s, user %s' % (
            _type, fitbit_user, sdat))
        raise Ignore()

    fbusers = UserFitbit.objects.filter(fitbit_user=fitbit_user)
    default_period = utils.get_setting('FITAPP_DEFAULT_PERIOD')
    if default_period:
         dates = {'base_date': 'today', 'period': default_period}
    else:
        dates = {'base_date': 'today', 'period': 'max'}
    if date:
        dates = {'base_date': date, 'end_date': date}
    try:
        for fbuser in fbusers:
            data = utils.get_fitbit_data(fbuser, _type, **dates)
            if utils.get_setting('FITAPP_GET_INTRADAY'):
                tz_offset = utils.get_fitbit_profile(fbuser,
                                                     'offsetFromUTCMillis')
                tz_offset = tz_offset / 3600 / 1000  # Converted to hours
            for datum in data:
                # Create new record or update existing record
                date = parser.parse(datum['dateTime'])
                value = datum['value']
                # Don't create unnecessary records
                if int(float(value)) == 0:
                    continue
                if _type.intraday_support and \
                        utils.get_setting('FITAPP_GET_INTRADAY'):
                    resources = TimeSeriesDataType.objects.filter(
                        intraday_support=True)
                    for i, _type in enumerate(resources):
                        # Offset each call by 2 seconds so they don't bog down
                        # the server
                        get_intraday_data.apply_async(
                            (fbuser.fitbit_user, _type.category,
                             _type.resource, date, tz_offset),
                            countdown=(2 * i))
                tsd, created = TimeSeriesData.objects.get_or_create(
                    user=fbuser.user, resource_type=_type, date=date,
                    intraday=False)
                tsd.value = value
                tsd.save()
        # Release the lock
        cache.delete(lock_id)
    except HTTPTooManyRequests:
        # We have hit the rate limit for the user, retry when it's reset,
        # according to the reply from the failing API call
        e = sys.exc_info()[1]
        logger.debug('Rate limit reached, will try again in %s seconds' %
                     e.retry_after_secs)
        raise get_time_series_data.retry(exc=e, countdown=e.retry_after_secs)
    except HTTPBadRequest:
        # If the resource is elevation or floors, we are just getting this
        # error because the data doesn't exist for this user, so we can ignore
        # the error
        if not ('elevation' in resource or 'floors' in resource):
            exc = sys.exc_info()[1]
            logger.exception("Exception updating data: %s" % exc)
            raise Reject(exc, requeue=False)
    except Exception:
        exc = sys.exc_info()[1]
        logger.exception("Exception updating data: %s" % exc)
        raise Reject(exc, requeue=False)


@shared_task
def get_intraday_data(fitbit_user, cat, resource, date, tz_offset):
    """
    Get the user's intraday data for a specified date, convert to UTC prior to
    saving.

    The Fitbit API stipulates that intraday data can only be retrieved for one
    day at a time.
    """
    try:
        _type = TimeSeriesDataType.objects.get(category=cat, resource=resource)
    except TimeSeriesDataType.DoesNotExist:
        logger.exception("The resource %s in category %s doesn't exist" %
                         (resource, cat))
        raise Reject(sys.exc_info()[1], requeue=False)
    if not _type.intraday_support:
        logger.exception("The resource %s in category %s does not support "
                         "intraday time series" % (resource, cat))
        raise Reject(sys.exc_info()[1], requeue=False)

    # Create a lock so we don't try to run the same task multiple times
    sdat = date.strftime('%Y-%m-%d')
    lock_id = '{0}-lock-intraday-{1}-{2}-{3}'.format(__name__, fitbit_user,
                                                     _type, sdat)
    if not cache.add(lock_id, 'true', LOCK_EXPIRE):
        logger.debug('Already retrieving intraday %s data for date %s, user %s'
                     % (_type, fitbit_user, sdat))
        raise Ignore()

    fbusers = UserFitbit.objects.filter(fitbit_user=fitbit_user)
    dates = {'base_date': date, 'period': '1d'}
    try:
        for fbuser in fbusers:
            data = utils.get_fitbit_data(fbuser, _type, return_all=True,
                                         **dates)
            resource_path = _type.path().replace('/', '-')
            key = resource_path + "-intraday"
            if data[key]['datasetType'] != 'minute':
                logger.exception("The resource returned is not "
                                 "minute-level data")
                raise Reject(sys.exc_info()[1], requeue=False)
            intraday = data[key]['dataset']
            logger.info("Date for intraday task: {}".format(date))
            for minute in intraday:
                datetime = parser.parse(minute['time'], default=date)
                utc_datetime = datetime + timedelta(hours=tz_offset)
                utc_datetime = utc_datetime.replace(tzinfo=utc)
                value = minute['value']
                # Don't create unnecessary records
                if not utils.get_setting('FITAPP_SAVE_INTRADAY_ZERO_VALUES'):
                    if int(float(value)) == 0:
                        continue
                # Create new record or update existing
                tsd, created = TimeSeriesData.objects.get_or_create(
                    user=fbuser.user, resource_type=_type, date=utc_datetime,
                    intraday=True)
                tsd.value = value
                tsd.save()
        # Release the lock
        cache.delete(lock_id)
    except HTTPTooManyRequests:
        # We have hit the rate limit for the user, retry when it's reset,
        # according to the reply from the failing API call
        e = sys.exc_info()[1]
        logger.debug('Rate limit reached, will try again in %s seconds' %
                     e.retry_after_secs)
        raise get_intraday_data.retry(exc=e, countdown=e.retry_after_secs)
    except HTTPBadRequest:
        # If the resource is elevation or floors, we are just getting this
        # error because the data doesn't exist for this user, so we can ignore
        # the error
        if not ('elevation' in resource or 'floors' in resource):
            exc = sys.exc_info()[1]
            logger.exception("Exception updating intraday data: %s" % exc)
            raise Reject(exc, requeue=False)
    except Exception:
        exc = sys.exc_info()[1]
        logger.exception("Exception updating data: %s" % exc)
        raise Reject(exc, requeue=False)
