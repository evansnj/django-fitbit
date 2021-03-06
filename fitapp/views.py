import json

from dateutil import parser
from dateutil.relativedelta import relativedelta
from django.contrib.auth.decorators import login_required
from django.contrib.auth.signals import user_logged_in
from django.core.exceptions import ImproperlyConfigured
from django.core.urlresolvers import reverse
from django.db import IntegrityError
from django.dispatch import receiver
from django.http import HttpResponse, Http404
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from fitbit.exceptions import (HTTPUnauthorized, HTTPForbidden, HTTPConflict,
                               HTTPServerError)

from . import forms
from . import utils
from .models import UserFitbit, TimeSeriesData, TimeSeriesDataType
from .tasks import get_time_series_data, subscribe, unsubscribe

try:
    from types import StringType, UnicodeType
    STRING_TYPES = [StringType, UnicodeType]
except ImportError:  # Python 3
    STRING_TYPES = [str]


class conditional_decorator:
    """
    Code taken from:
    http://stackoverflow.com/questions/10724854/how-to-do-a-conditional-decorator-in-python-2-6

    usage:
    @conditional_decorator(timeit, doing_performance_analysis)
    def foo():
        time.sleep(2)
    """
    def __init__(self, dec, condition):
        self.decorator = dec
        self.condition = condition

    def __call__(self, func):
        if not self.condition:
            # Return the function unchanged, not decorated.
            return func
        return self.decorator(func)


@conditional_decorator(login_required, utils.get_setting('FITAPP_LOGIN_REQUIRED'))
def login(request):
    """
    Begins the OAuth authentication process by obtaining a Request Token from
    Fitbit and redirecting the user to the Fitbit site for authorization.

    When the user has finished at the Fitbit site, they will be redirected
    to the :py:func:`fitapp.views.complete` view.

    If 'next' is provided in the GET data, it is saved in the session so the
    :py:func:`fitapp.views.complete` view can redirect the user to that URL
    upon successful authentication.

    URL name:
        `fitbit-login`
    """
    next_url = request.GET.get('next', None)
    if next_url:
        request.session['fitbit_next'] = next_url
    else:
        request.session.pop('fitbit_next', None)

    callback_uri = request.build_absolute_uri(reverse('fitbit-complete'))
    fb = utils.create_fitbit(callback_uri=callback_uri)
    token = fb.client.fetch_request_token()
    token_url = fb.client.authorize_token_url()
    request.session['token'] = token
    return redirect(token_url)


@conditional_decorator(login_required, utils.get_setting('FITAPP_LOGIN_REQUIRED'))
def complete(request):
    """
    After the user authorizes us, Fitbit sends a callback to this URL to
    complete authentication.

    If there was an error, the user is redirected again to the `error` view.

    If the authorization was successful, the credentials are stored for us to
    use later, and the user is redirected. If 'next_url' is in the request
    session, the user is redirected to that URL. Otherwise, they are
    redirected to the URL specified by the setting
    :ref:`FITAPP_LOGIN_REDIRECT`.

    If :ref:`FITAPP_SUBSCRIBE` is set to True, add a subscription to user
    data at this time.

    Requires the pk of the user or model that will be associated with fitapp
    data to be inserted into request.session['fb_user_id'] prior to calling
    the view.

    URL name:
        `fitbit-complete`
    """
    user_model = UserFitbit.user.field.rel.to

    fb = utils.create_fitbit()
    try:
        token = request.session.pop('token')
        verifier = request.GET.get('oauth_verifier')
        fb_user_id = int(request.session.get('fb_user_id'))
        user = user_model.objects.get(pk=fb_user_id)
    except KeyError:
        return redirect(reverse('fitbit-error'))
    try:
        fb.client.fetch_access_token(verifier, token=token)
    except:
        return redirect(reverse('fitbit-error'))

    if UserFitbit.objects.filter(fitbit_user=fb.client.user_id).exists():
        return redirect(reverse('fitbit-error'))

    fbuser, _ = UserFitbit.objects.get_or_create(user=user)
    fbuser.auth_token = fb.client.resource_owner_key
    fbuser.auth_secret = fb.client.resource_owner_secret
    fbuser.fitbit_user = fb.client.user_id
    fbuser.save()

    # Add the Fitbit user info to the session
    request.session['fitbit_profile'] = fb.user_profile_get()
    if utils.get_setting('FITAPP_SUBSCRIBE'):
        try:
            SUBSCRIBER_ID = utils.get_setting('FITAPP_SUBSCRIBER_ID')
        except ImproperlyConfigured:
            return redirect(reverse('fitbit-error'))
        subscribe.apply_async((fbuser.fitbit_user, str(SUBSCRIBER_ID)),
                              countdown=5)
        # Create tasks for all data in all data types
        for i, _type in enumerate(TimeSeriesDataType.objects.all()):
            # Delay execution for a few seconds to speed up response
            # Offset each call by 2 seconds so they don't bog down the server
            get_time_series_data.apply_async(
                (fbuser.fitbit_user, _type.category, _type.resource,),
                countdown=10 + (i * 5))

    next_url = request.session.pop('fitbit_next', None) or utils.get_setting(
        'FITAPP_LOGIN_REDIRECT')
    return redirect(next_url)


@receiver(user_logged_in)
def create_fitbit_session(sender, request, user, **kwargs):
    """ If the user is a fitbit user, update the profile in the session. """

    if user.is_authenticated() and utils.is_integrated(user) and \
            user.is_active:
        fbuser = UserFitbit.objects.filter(user=user)
        if fbuser.exists():
            fb = utils.create_fitbit(**fbuser[0].get_user_data())
            try:
                request.session['fitbit_profile'] = fb.user_profile_get()
            except:
                pass


@conditional_decorator(login_required, utils.get_setting('FITAPP_LOGIN_REQUIRED'))
def error(request):
    """
    The user is redirected to this view if we encounter an error acquiring
    their Fitbit credentials. It renders the template defined in the setting
    :ref:`FITAPP_ERROR_TEMPLATE`. The default template, located at
    *fitapp/error.html*, simply informs the user of the error::

        <html>
            <head>
                <title>Fitbit Authentication Error</title>
            </head>
            <body>
                <h1>Fitbit Authentication Error</h1>

                <p>We encontered an error while attempting to authenticate you
                through Fitbit.</p>
            </body>
        </html>

    URL name:
        `fitbit-error`
    """
    return render(request, utils.get_setting('FITAPP_ERROR_TEMPLATE'), {})


@conditional_decorator(login_required, utils.get_setting('FITAPP_LOGIN_REQUIRED'))
def logout(request):
    """Forget this user's Fitbit credentials.

    If the request has a `next` parameter, the user is redirected to that URL.
    Otherwise, they're redirected to the URL defined in the setting
    :ref:`FITAPP_LOGOUT_REDIRECT`.

    URL name:
        `fitbit-logout`
    """
    user_model = UserFitbit.user.field.rel.to
    try:
        fb_user_id = request.session.get('fb_user_id')
        user = user_model.objects.get(pk=fb_user_id)
    except KeyError:
        return redirect(reverse('fitbit-error'))
    try:
        fbuser = user.userfitbit
    except UserFitbit.DoesNotExist:
        pass
    else:
        if utils.get_setting('FITAPP_SUBSCRIBE'):
            try:
                SUBSCRIBER_ID = utils.get_setting('FITAPP_SUBSCRIBER_ID')
            except ImproperlyConfigured:
                return redirect(reverse('fitbit-error'))
            unsubscribe.apply_async(kwargs=fbuser.get_user_data(), countdown=5)
        fbuser.delete()
    next_url = request.GET.get('next', None) or utils.get_setting(
        'FITAPP_LOGOUT_REDIRECT')
    return redirect(next_url)


@csrf_exempt
def update(request):
    """Receive notification from Fitbit.

    Loop through the updates and create celery tasks to get the data.
    More information here:
    https://dev.fitbit.com/docs/subscriptions/

    The updates can come in two ways (via POST):
      1. A json body in a POST request
      2. A json file in a form POST

    A GET request indicates a subscription verification. More information here:
    https://dev.fitbit.com/docs/subscriptions/#verify-a-subscriber

    URL name:
        `fitbit-update`
    """

    if request.method == 'POST':
        try:
            body = request.body
            if request.FILES and 'updates' in request.FILES:
                body = request.FILES['updates'].read()
            updates = json.loads(body.decode('utf8'))
            # Create a celery task for each data type in the update
            for update in updates:
                cat = getattr(TimeSeriesDataType, update['collectionType'])
                resources = TimeSeriesDataType.objects.filter(category=cat)
                for i, _type in enumerate(resources):
                    # Offset each call by 2 seconds so they don't bog down the
                    # server
                    get_time_series_data.apply_async(
                        (update['ownerId'], _type.category, _type.resource,),
                        {'date': parser.parse(update['date'])},
                        countdown=(2 * i))
        except:
            return redirect(reverse('fitbit-error'))

        return HttpResponse(status=204)
    # A verification request
    elif request.method == 'GET':
        # Requests from Fitbit have 'verify' parameter
        request_code = request.GET.get('verify', '')
        if not request_code:
            raise Http404
        known_code = utils.get_setting('FITAPP_VERIFICATION_CODE')
        if request_code == known_code:
            return HttpResponse(status=204)
    raise Http404


def make_response(code=None, objects=[]):
    """AJAX helper method to generate a response"""

    data = {
        'meta': {'total_count': len(objects), 'status_code': code},
        'objects': objects,
    }
    return HttpResponse(json.dumps(data))


def normalize_date_range(request, fitbit_data):
    """Prepare a fitbit date range for django database access. """

    result = {}
    base_date = fitbit_data['base_date']
    if base_date == 'today':
        now = timezone.now()
        if 'fitbit_profile' in request.session.keys():
            tz = request.session['fitbit_profile']['user']['timezone']
            now = timezone.pytz.timezone(tz).normalize(timezone.now())
        base_date = now.date().strftime('%Y-%m-%d')
    result['date__gte'] = base_date

    if 'end_date' in fitbit_data.keys():
        result['date__lte'] = fitbit_data['end_date']
    else:
        period = fitbit_data['period']
        if period != 'max':
            if type(base_date) in STRING_TYPES:
                start = parser.parse(base_date)
            else:
                start = base_date
            if 'y' in period:
                kwargs = {'years': int(period.replace('y', ''))}
            elif 'm' in period:
                kwargs = {'months': int(period.replace('m', ''))}
            elif 'w' in period:
                kwargs = {'weeks': int(period.replace('w', ''))}
            elif 'd' in period:
                kwargs = {'days': int(period.replace('d', ''))}
            end_date = start + relativedelta(**kwargs)
            result['date__lte'] = end_date.strftime('%Y-%m-%d')

    return result


@require_GET
def get_steps(request):
    """An AJAX view that retrieves this user's step data from Fitbit.

    This view has been deprecated. Use `get_data` instead.

    URL name:
        `fitbit-steps`
    """

    return get_data(request, 'activities', 'steps')


@require_GET
def get_data(request, category, resource):
    """An AJAX view that retrieves this user's data from Fitbit.

    This view may only be retrieved through a GET request. The view can
    retrieve data from either a range of dates, with specific start and end
    days, or from a time period ending on a specific date.

    Requires the pk of the user or model that is associated with the fitapp data
    to be inserted into request.session['fb_user_id'] prior to calling the view.

    The two parameters, category and resource, determine which type of data
    to retrieve. The category parameter can be one of: foods, activities,
    sleep, and body. It's the first part of the path in the items listed at
    https://wiki.fitbit.com/display/API/API-Get-Time-Series
    The resource parameter should be the rest of the path.

    To retrieve a specific time period, two GET parameters are used:

        :period: A string describing the time period, ending on *base_date*,
            for which to retrieve data - one of '1d', '7d', '30d', '1w', '1m',
            '3m', '6m', '1y', or 'max.
        :base_date: The last date (in the format 'yyyy-mm-dd') of the
            requested period. If not provided, then *base_date* is
            assumed to be today.

    To retrieve a range of dates, two GET parameters are used:

        :base_date: The first day of the range, in the format 'yyyy-mm-dd'.
        :end_date: The final day of the range, in the format 'yyyy-mm-dd'.

    The response body contains a JSON-encoded map with two items:

        :objects: an ordered list (from oldest to newest) of daily data
            for the requested period. Each day is of the format::

               {'dateTime': 'yyyy-mm-dd', 'value': 123}

           where the user has *value* on *dateTime*.
        :meta: a map containing two things: the *total_count* of objects, and
            the *status_code* of the response.

    When everything goes well, the *status_code* is 100 and the requested data
    is included. However, there are a number of things that can 'go wrong'
    with this call. For each type of error, we return an empty data list with
    a *status_code* to describe what went wrong on our end:

        :100: OK - Response contains JSON data.
        :101: User is not logged in.
        :102: User is not integrated with Fitbit.
        :103: Fitbit authentication credentials are invalid and have been
            removed.
        :104: Invalid input parameters. Either *period* or *end_date*, but not
            both, must be supplied. *period* should be one of [1d, 7d, 30d,
            1w, 1m, 3m, 6m, 1y, max], and dates should be of the format
            'yyyy-mm-dd'.
        :105: User exceeded the Fitbit limit of 150 calls/hour.
        :106: Fitbit error - please try again soon.

    See also the `Fitbit API doc for Get Time Series
    <https://wiki.fitbit.com/display/API/API-Get-Time-Series>`_.

    URL name:
        `fitbit-data`
    """
    user_model = UserFitbit.user.field.rel.to

    try:
        fb_user_id = int(request.session.get('fb_user_id'))
        user = user_model.objects.get(pk=fb_user_id)
    except KeyError:
        return make_response(102)

    # Manually check that user is logged in and integrated with Fitbit.
    user = request.user
    try:
        resource_type = TimeSeriesDataType.objects.get(
            category=getattr(TimeSeriesDataType, category), resource=resource)
    except:
        return make_response(104)

    fitapp_subscribe = utils.get_setting('FITAPP_SUBSCRIBE')
    if not user.is_authenticated() or not user.is_active:
        return make_response(101)
    if not fitapp_subscribe and not utils.is_integrated(user):
        return make_response(102)

    base_date = request.GET.get('base_date', None)
    period = request.GET.get('period', None)
    end_date = request.GET.get('end_date', None)
    if period and not end_date:
        form = forms.PeriodForm({'base_date': base_date, 'period': period})
    elif end_date and not period:
        form = forms.RangeForm({'base_date': base_date, 'end_date': end_date})
    else:
        # Either end_date or period, but not both, must be specified.
        return make_response(104)

    fitbit_data = form.get_fitbit_data()
    if not fitbit_data:
        return make_response(104)

    if fitapp_subscribe:
        # Get the data directly from the database.
        date_range = normalize_date_range(request, fitbit_data)
        existing_data = TimeSeriesData.objects.filter(
            user=user, resource_type=resource_type, **date_range)
        simplified_data = [{'value': d.value, 'dateTime': d.string_date()}
                           for d in existing_data]
        return make_response(100, simplified_data)

    # Request data through the API and handle related errors.
    fbuser = UserFitbit.objects.get(user=user)
    try:
        data = utils.get_fitbit_data(fbuser, resource_type, **fitbit_data)
    except (HTTPUnauthorized, HTTPForbidden):
        # Delete invalid credentials.
        fbuser.delete()
        return make_response(103)
    except HTTPConflict:
        return make_response(105)
    except HTTPServerError:
        return make_response(106)
    except:
        # Other documented exceptions include TypeError, ValueError,
        # HTTPNotFound, and HTTPBadRequest. But they shouldn't occur, so we'll
        # send a 500 and check it out.
        raise

    return make_response(100, data)
