# (C) Copyright 2020 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#


import os
import time
import urllib
from collections.abc import Mapping

import requests
import yaml
from climetlab import load_source
from climetlab.sources.multi import MultiSource
from climetlab.sources.prompt import APIKeyPrompt

# See https://eumetsatspace.atlassian.net/wiki/spaces/DSDS/pages/315818088/Using+the+APIs


class EumetsatAPIKeyPrompt(APIKeyPrompt):

    register_or_sign_in_url = "https://eoportal.eumetsat.int/"
    retrieve_api_key_url = "https://api.eumetsat.int/api-key/"

    prompts = [
        dict(
            name="consumer_key",
            example="aEfX6e1AvizULa48eo9R1v9A56md",
            title="Consumer key",
            hidden=True,
            validate=r"\w{28}",
        ),
        dict(
            name="consumer_secret",
            example="Uiaz51e8XAfmA969o1vR4aELdev6",
            title="Consumer secret",
            hidden=True,
            validate=r"\w{28}",
        ),
    ]

    rcfile = "~/.eumetsatapirc"


class Token(Mapping):
    def __init__(self):
        with open(os.path.expanduser(EumetsatAPIKeyPrompt.rcfile)) as f:
            self._credentials = yaml.safe_load(f)

        self._token = {"expires_in": 0}
        self._last = 0

    @property
    def token(self):

        now = time.time()
        if now - self._last > self._token["expires_in"] - 10:

            r = requests.post(
                "https://api.eumetsat.int/token",
                data={"grant_type": "client_credentials"},
                auth=requests.auth.HTTPBasicAuth(
                    self._credentials["consumer_key"],
                    self._credentials["consumer_secret"],
                ),
            )

            r.raise_for_status()

            self._last = now
            self._token = r.json()

        return self._token["access_token"]

    def __repr__(self):
        return self.token

    # These methods make the token a lazy dictionary,
    # where the token value is evaluated at the last minute
    # so it does not expire
    def __len__(self):
        return 1

    def __getitem__(self, k):
        assert k == "Authorization"
        return f"Bearer {self.token}"

    def __iter__(self):
        return iter(["Authorization"])


# Try with EO:EUM:DAT:METOP:GLB-SST-NC


class Client:
    def __init__(self):
        self.token = Token()

    def features(
        self,
        collection,
        start_date=None,
        end_date=None,
        polygon=None,
    ):
        query = {
            "format": "json",
            "pi": collection,
        }

        if start_date is not None:
            if isinstance(start_date, str):
                query["dtstart"] = start_date
            else:
                query["dtstart"] = start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        if end_date is not None:
            if isinstance(start_date, str):
                query["dtend"] = end_date
            else:
                query["dtend"] = end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        if polygon is not None:
            geo = ",".join([f"{pt[0]} {pt[1]}" for pt in polygon])
            query["geo"] = f"POLYGON(({geo}))"

        search_url = "https://api.eumetsat.int/data/search-products/os"

        query["si"] = 0
        query["c"] = 1000

        while True:

            r = requests.get(search_url, query)
            r.raise_for_status()
            result = r.json()

            features = result["features"]

            if not features:
                break

            yield from features

            query["si"] += len(features)

    def products(self, *args, **kwargs):
        sources = []
        for feature in self.features(*args, **kwargs):
            pid = urllib.parse.quote(feature["properties"]["identifier"])
            cid = urllib.parse.quote(feature["properties"]["parentIdentifier"])
            url = f"https://api.eumetsat.int/data/download/collections/{cid}/products/{pid}"
            size = feature["properties"]["productInformation"]["size"]
            sources.append(
                load_source(
                    "url",
                    url,
                    # We make that a callable as we may dowload that file when the token needs to be refreshed
                    http_headers=self.token,
                    fake_headers={
                        # HEAD is not allowed, and we know the size
                        "content-length": 1024 * size,
                        # We know the size to the nearest kb, so turn off size checking
                        "content-encoding": "unknown",
                    },
                    # Load lazily so we can do parallel downloads
                    lazily=True,
                ),
            )

        return load_source("multi", sources)


def client():
    prompt = EumetsatAPIKeyPrompt()
    prompt.check()

    try:
        return Client()
    except Exception as e:
        if ".eumetsatapirc" in str(e):
            prompt.ask_user_and_save()
            return Client()

        raise


class EumetsatRetriever(MultiSource):
    def __init__(self, collection, *args, **kwargs):
        assert isinstance(collection, str)

        c = client()
        super().__init__(
            c.products(
                collection,
                *args,
                **kwargs,
            )
        )


source = EumetsatRetriever
