"""
The purpose of this function is to trigger an airflow DAG to run within Google Cloud Composer. Many of the ideas
and much of the original code can be attributed to the folks at this forum: https://groups.google.com/forum/#!topic/cloud-composer-discuss/PGW81QH9D0w

"""
import google.auth
import google.auth.app_engine
import google.auth.compute_engine.credentials
import google.auth.iam
from google.auth.transport.requests import Request
import google.oauth2.credentials
import google.oauth2.service_account
import requests
import requests_toolbelt.adapters.appengine
import datetime
import json
import base64
import random

def bucket_trigger(client_id=None, webserver_id=None, dag_name=None):

    IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
    OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'

    #follow this link to get Client ID: 
    #https://cloud.google.com/composer/docs/how-to/using/triggering-with-gcf
    client_id=""
    # This should be part of your webserver's URL:
    # {tenant-project-id}.appspot.com
    webserver_id="-tp"
    #name of dag to trigger
    dag_name="dag_name"

    def get_google_open_id_connect_token(service_account_credentials):
        """Get an OpenID Connect token issued by Google for the service account.
        This function:
        1. Generates a JWT signed with the service account's private key
            containing a special "target_audience" claim.
        2. Sends it to the OAUTH_TOKEN_URI endpoint. Because the JWT in #1
            has a target_audience claim, that endpoint will respond with
            an OpenID Connect token for the service account -- in other words,
            a JWT signed by *Google*. The aud claim in this JWT will be
            set to the value from the target_audience claim in #1.
        For more information, see
        https://developers.google.com/identity/protocols/OAuth2ServiceAccount .
        The HTTP/REST example on that page describes the JWT structure and
        demonstrates how to call the token endpoint. (The example on that page
        shows how to get an OAuth2 access token; this code is using a
        modified version of it to get an OpenID Connect token.)
        """
        service_account_jwt = (
            service_account_credentials._make_authorization_grant_assertion())
        request = google.auth.transport.requests.Request()
        body = {
            'assertion': service_account_jwt,
            'grant_type': google.oauth2._client._JWT_GRANT_TYPE,
        }
        token_response = google.oauth2._client._token_endpoint_request(
            request, OAUTH_TOKEN_URI, body)
        return token_response['id_token']

    def make_iap_request(url, client_id, method='GET', **kwargs):
        """Makes a request to an application protected by Identity-Aware Proxy.
        Args:
        url: The Identity-Aware Proxy-protected URL to fetch.
        client_id: The client ID used by Identity-Aware Proxy.
        method: The request method to use
                ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
        **kwargs: Any of the parameters defined for the request function:
                    https://github.com/requests/requests/blob/master/requests/api.py
        Returns:
        The page body, or raises an exception if the page couldn't be retrieved.
        """
        bootstrap_credentials, _ = google.auth.default(scopes=[IAM_SCOPE])
        if isinstance(bootstrap_credentials, google.oauth2.credentials.Credentials):
            raise Exception('make_iap_request is only supported for service accounts.')
        elif isinstance(bootstrap_credentials, google.auth.app_engine.Credentials):
            requests_toolbelt.adapters.appengine.monkeypatch()

        bootstrap_credentials.refresh(Request())

        signer_email = bootstrap_credentials.service_account_email
        if isinstance(bootstrap_credentials,
                    google.auth.compute_engine.credentials.Credentials):
            signer = google.auth.iam.Signer(Request(), bootstrap_credentials, signer_email)
        else:
            signer = bootstrap_credentials.signer

        service_account_credentials = google.oauth2.service_account.Credentials(
            signer, signer_email, token_uri=OAUTH_TOKEN_URI, additional_claims={
                'target_audience': client_id
            })

        google_open_id_connect_token = get_google_open_id_connect_token(
            service_account_credentials)

        resp = requests.request(
            method, url,
            headers={'Authorization': 'Bearer {}'.format(
                google_open_id_connect_token)}, **kwargs)
        if resp.status_code == 403:
            raise Exception('Service account {} does not have permission to '
                            'access the IAP-protected application.'.format(
                                signer_email))
        elif resp.status_code != 200:
            raise Exception(
                'Bad response from application: {!r} / {!r} / {!r}'.format(
                    resp.status_code, resp.headers, resp.text))
        else:
            return resp.text

    def get_run_id(dag_name=None):
        """
        This function builds a nice run_id, format dagName_YYYYMMDDHmS_randomInt
        Returns:
        A string with the run_id
        """
        myDate = datetime.datetime.now()
        dateIntStr = str(myDate.year) + ("0" + str(myDate.month))[-2:] + ("0" + str(myDate.day))[-2:] 
        dateIntStr = dateIntStr + ("0" + str(myDate.hour))[-2:] + ("0" + str(myDate.minute))[-2:]
        dateIntStr = dateIntStr + ("0" + str(myDate.second))[-2:] + "_" + str(random.randint(10, 999))

        if dag_name:
            run_id = str(dag_name) + "_" + dateIntStr
        else:
            run_id = 'TEST_' + dateIntStr
        return run_id

    def run_dag(client_id=None, webserver_id=None, dag_name=None, data_dict=None, run_id=None):
        """
        This main function calls the make_iap_request function 
        and then prints the output of the function. The make_iap_request function
        demonstrates how to authenticate to Identity-Aware Proxy using a service account.
        Returns:
        A dictionary with basic info on how things went, or raises an exception if the page couldn't be retrieved.
        """
        try:
            clientId = client_id 
            webserverId = webserver_id
            dagName = dag_name
            data = {'var1': 'val1', 'var2': 'val2'}

            return_dict = dict()
            return_dict['client_id'] = client_id
            return_dict['clientId'] = clientId
            return_dict['webserver_id'] = webserver_id
            return_dict['webserverId'] = webserverId
            return_dict['dag_name'] = dag_name
            return_dict['dagName'] = dagName
            return_dict['data'] = data

            url = 'https://' + webserverId + '.appspot.com/api/experimental/dags/' + dagName + '/dag_runs'
            return_dict['url'] = url

            if run_id is None:
                run_id = get_run_id(dag_name)

            return_dict['run_id'] = run_id

            print(("run_id: " + str(run_id)))

            crypt_data = json.dumps(data)
            return_dict['crypt_data'] = crypt_data
            conf = {'data': crypt_data}

            payload = {
                'run_id': run_id,
                'conf': json.dumps(conf),
            }
            iap_output = make_iap_request(url, clientId, method='POST', data=json.dumps(payload))
            return_dict['iap_output'] = iap_output

            return return_dict

        except Exception as e:
            errorStr = 'ERROR (run_dag): ' + str(e)
            print(errorStr)
            raise

    run_dag(client_id, webserver_id, dag_name)


