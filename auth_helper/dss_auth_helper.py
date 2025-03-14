import json
import logging
from datetime import datetime, timedelta
from os import environ as env

import requests
from dotenv import find_dotenv, load_dotenv

from .common import get_redis

logger = logging.getLogger("django")

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

    class AuthorityCredentialsGetter:
        """
        A class to handle the retrieval and caching of authority credentials.
        Methods
        -------
        __init__():
            Initializes the AuthorityCredentialsGetter with a Redis connection and the current datetime.
        get_cached_credentials(audience: str, token_type: str):
            Retrieves cached credentials if available and valid, otherwise fetches new credentials and caches them.
        _get_credentials(audience: str, token_type: str):
            Determines the type of credentials to fetch based on the token type.
        _cache_credentials(cache_key: str, credentials: dict):
            Caches the credentials in Redis with a specified expiration time.
        _get_rid_credentials(audience: str):
            Fetches RID (Remote ID) credentials for the given audience.
        _get_scd_credentials(audience: str):
            Fetches SCD (Strategic Coordination) credentials for the given audience.
        _get_cmsa_credentials(audience: str):
            Fetches CMSA (Conformance Monitoring Service Area) credentials for the given audience.
        _request_credentials(audience: str, scope: str):
            Makes a request to the authentication service to retrieve credentials for the given audience and scope.
        """

        def __init__(self):
            self.redis = get_redis()
            self.now = datetime.now()

        def get_cached_credentials(self, audience: str, token_type: str):
            token_suffix = "_auth_rid_token" if token_type == "rid" else "_auth_scd_token"
            cache_key = audience + token_suffix
            token_details = self.redis.get(cache_key)

            if token_details:
                token_details = json.loads(token_details)
                created_at = token_details["created_at"]
                set_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%f")
                if self.now < (set_date + timedelta(minutes=58)):
                    return token_details["credentials"]

            credentials = self._get_credentials(audience, token_type)
            self._cache_credentials(cache_key, credentials)
            return credentials

        def _get_credentials(self, audience: str, token_type: str):
            if token_type == "rid":
                return self._get_rid_credentials(audience)
            elif token_type == "scd":
                return self._get_scd_credentials(audience)
            elif token_type == "cmsa":
                return self._get_cmsa_credentials(audience)
            else:
                raise ValueError("Invalid token type")

        def _cache_credentials(self, cache_key: str, credentials: dict):
            self.redis.set(
                cache_key,
                json.dumps({"credentials": credentials, "created_at": self.now.isoformat()}),
            )
            self.redis.expire(cache_key, timedelta(minutes=58))

        def _get_rid_credentials(self, audience: str):
            return self._request_credentials(audience, ["rid.service_provider", "rid.display_provider"])

        def _get_scd_credentials(self, audience: str):
            return self._request_credentials(audience, ["utm.strategic_coordination", "utm.conformance_monitoring_sa"])

        def _get_cmsa_credentials(self, audience: str):
            return self._request_credentials(audience, ["utm.conformance_monitoring_sa"])

        def _request_credentials(self, audience: str, scopes: list[str]):
            issuer = audience if audience == "localhost" else None
            scopes_str = " ".join(scopes)

            if audience in ["localhost", "host.docker.internal"]:
                payload = {
                    "grant_type": "client_credentials",
                    "intended_audience": env.get("DSS_SELF_AUDIENCE"),
                    "scope": scopes_str,
                    "issuer": issuer,
                }
            else:
                payload = {
                    "grant_type": "client_credentials",
                    "client_id": env.get("AUTH_DSS_CLIENT_ID"),
                    "client_secret": env.get("AUTH_DSS_CLIENT_SECRET"),
                    "intended_audience": audience,
                    "scope": scopes_str,
                }

            url = env.get("DSS_AUTH_URL") + env.get("DSS_AUTH_TOKEN_ENDPOINT")
            token_data = requests.get(url, params=payload)
            return token_data.json()
