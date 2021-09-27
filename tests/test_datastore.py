#!/usr/bin/env python3# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import datetime

import climetlab as cml


def test_load_source_1():

    ds = cml.load_source(
        "eumetsat-datastore",
        collection="EO:EUM:DAT:METOP:GLB-SST-NC",
        start_date="2016-07-23T17:58:00Z",
        end_date="2016-07-24T06:01:03Z",
    )
    xds = ds.to_xarray()
    print(xds)

def f():
    print('dddd')

def test_load_source_2():

    ds = cml.load_source(
        "eumetsat-datastore",
        _observer=f,
        collection="EO:EUM:DAT:METOP:GLB-SST-NC",
        start_date="2016-07-23T17:58:00Z",
        end_date="2016-07-24T06:01:03Z",
    )

    assert ds.to_datetime_list() == [
        datetime.datetime(2016, 7, 23, 12, 0),
        datetime.datetime(2016, 7, 24, 0, 0),
        datetime.datetime(2016, 7, 24, 12, 0),
    ]

    assert ds.to_bounding_box().as_tuple() == (
        89.974609375,
        -179.974609375,
        -89.974609375,
        179.974609375,
    )

    ds = cml.load_source(
        "eumetsat-datastore",
        collection="EO:EUM:DAT:METOP:GLB-SST-NC",
        start_date="2016-07-24T00:00:00Z",
        end_date="2016-07-24T00:00:00Z",
    )

    assert ds.to_datetime() == datetime.datetime(2016, 7, 24, 0, 0)

    assert ds.to_bounding_box().as_tuple() == (
        89.974609375,
        -179.974609375,
        -89.974609375,
        179.974609375,
    )


if __name__ == "__main__":
    from climetlab.testing import main

    main(globals())
