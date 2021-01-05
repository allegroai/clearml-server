#!/usr/bin/env python
from __future__ import print_function

import argparse
import json
import os
import sys
import time
from itertools import groupby
from operator import itemgetter

import pyhocon
import six
import yaml
from colors import color
from jsonschema import validate, ValidationError as JSONSchemaValidationError
from jsonschema.validators import validator_for
from pathlib import Path
from pyparsing import ParseBaseException

LINTER_URL = "https://www.jsonschemavalidator.net/"


class LocalStorage(object):
    def __init__(self, driver):
        self.driver = driver

    def __len__(self):
        return self.driver.execute_script("return window.localStorage.length;")

    def items(self):
        return self.driver.execute_script(
            """
            var ls = window.localStorage, items = {};
            for (var i = 0, k; i < ls.length; ++i)
                items[k = ls.key(i)] = ls.getItem(k);
            return items;
            """
        )

    def keys(self):
        return self.driver.execute_script(
            """
            var ls = window.localStorage, keys = [];
            for (var i = 0; i < ls.length; ++i)
                keys[i] = ls.key(i);
            return keys;
            """
        )

    def get(self, key):
        return self.driver.execute_script(
            "return window.localStorage.getItem(arguments[0]);", key
        )

    def remove(self, key):
        self.driver.execute_script("window.localStorage.removeItem(arguments[0]);", key)

    def clear(self):
        self.driver.execute_script("window.localStorage.clear();")

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __setitem__(self, key, value):
        self.driver.execute_script(
            "window.localStorage.setItem(arguments[0], arguments[1]);", key, value
        )

    def __contains__(self, key):
        return key in self.keys()

    def __iter__(self):
        return iter(self.keys())

    def __repr__(self):
        return repr(self.items())


class ValidationError(Exception):

    def __init__(self, *args):
        super(ValidationError, self).__init__(*args)
        self.message = self.args[0]

    def report(self, schema_file):
        message = color(schema_file, fg='red')
        if self.message:
            message += ": {}".format(self.message)
        print(message)


class InvalidFile(ValidationError):
    """
    InvalidFile
    Wraps other exceptions that occur in file validation

    :param message: message to display
    """

    def __init__(self, message):
        super(InvalidFile, self).__init__(message)
        exc_type, _, _ = self.exc_info = sys.exc_info()
        if exc_type:
            self.message = "{}: {}".format(exc_type.__name__, message)

    def raise_original(self):
        six.reraise(*self.exc_info)


def load_hocon(name):
    """
    load_hocon
    load configuration from file

    :param name: file path
    """
    return pyhocon.ConfigFactory.parse_file(name).as_plain_ordered_dict()


def validate_ascii_only(name):
    invalid_char = next(
        (
            (line_num, column, char)
            for line_num, line in enumerate(Path(name).read_text().splitlines())
            for column, char in enumerate(line)
            if ord(char) not in range(128)
        ),
        None,
    )
    if invalid_char:
        line, column, char = invalid_char
        raise ValidationError(
            "file contains non-ascii character {!r} in line {} pos {}".format(
                char, line, column
            )
        )


def validate_file(meta, name):
    """
    validate_file
    validate file according to meta-scheme

    :param meta: meta-scheme
    :param name: file path
    """
    validate_ascii_only(name)
    try:
        schema = load_hocon(name)
    except ParseBaseException as e:
        raise InvalidFile(repr(e))

    try:
        validate(schema, meta)
        return schema
    except JSONSchemaValidationError as e:
        path = "->".join(e.absolute_path)
        message = "{}: {}".format(path, e.args[0])
        raise InvalidFile(message)
    except Exception as e:
        raise InvalidFile(str(e))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+")
    parser.add_argument(
        "--linter", "-l", action="store_true", help="open jsonschema linter in browser"
    )
    parser.add_argument(
        "--raise",
        "-r",
        action="store_true",
        dest="raise_",
        help="raise first exception encountered and print traceback",
    )
    parser.add_argument(
        "--detect-collisions",
        action="store_true",
        help="detect objects with the same name in different modules",
    )
    return parser.parse_args()


def open_linter(driver, meta, schema):
    driver.maximize_window()
    driver.get(LINTER_URL)
    storage = LocalStorage(driver)
    storage["jsonText"] = json.dumps(schema, indent=4)
    storage["schemaText"] = json.dumps(meta, indent=4)
    driver.refresh()


class LazyDriver(object):
    def __init__(self):
        self._driver = None
        try:
            from selenium import webdriver, common
        except ImportError:
            webdriver = None
            common = None
        self.webdriver = webdriver
        self.common = common

    def __getattr__(self, item):
        return getattr(self.driver, item)

    @property
    def driver(self):
        if self._driver:
            return self._driver
        if not (self.webdriver and self.common):
            print("selenium not installed: linter unavailable")
            return None

        for driver_type in self.webdriver.Chrome, self.webdriver.Firefox:
            try:
                self._driver = driver_type()
                break
            except self.common.exceptions.WebDriverException:
                pass
        else:
            print("No webdriver is found for chrome or firefox")

        return self._driver

    def wait(self):
        if not self._driver:
            return
        try:
            while True:
                self._driver.title
                time.sleep(0.5)
        except self.common.exceptions.WebDriverException:
            pass


def remove_description(dct):
    dct.pop("description", None)
    for value in dct.values():
        try:
            remove_description(value)
        except (TypeError, AttributeError):
            pass


def main(here: str):
    args = parse_args()
    meta = load_hocon(here + "/meta.conf")
    validator_for(meta).check_schema(meta)

    driver = LazyDriver()

    collisions = {}

    for schema_file in args.files:

        if Path(schema_file).name.startswith("_"):
            continue

        try:
            schema = validate_file(meta, schema_file)
        except InvalidFile as e:
            if args.linter and driver.driver:
                open_linter(driver, meta, load_hocon(schema_file))
            elif args.raise_:
                e.raise_original()

            e.report(schema_file)
        except ValidationError as e:
            e.report(schema_file)
        else:
            for def_name, value in schema.get("_definitions", {}).items():
                service_name = str(Path(schema_file).stem)
                remove_description(value)
                collisions.setdefault(def_name, {})[service_name] = value

    warning = color("warning", fg="red")

    if args.detect_collisions:
        for name, values in collisions.items():
            if len(values) <= 1:
                continue
            groups = [
                [service for (service, _) in pairs]
                for _, pairs in groupby(values.items(), itemgetter(1))
            ]
            if not groups:
                raise RuntimeError("Unknown error")
            print(
                "{}: collision for {}:\n{}".format(warning, name, yaml.dump(groups)),
                end="",
            )

    driver.wait()


if __name__ == "__main__":
    main(here=os.path.dirname(__file__))
