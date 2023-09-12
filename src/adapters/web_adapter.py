#!/usr/bin/env python3
from urllib.parse import urljoin

import requests

from bento.common.utils import removeTrailingSlash
from .base_adapter import BentoAdapter


class BentoWeb(BentoAdapter):
    """
    This adapter handles original files are publicly accessible on the web
    Pre-manifest file can contain Original URLs or file names and a common URL prefix given in a parameter

    Following method is required:
        - get_org_url
    """


    def __init__(self, url_prefix=None, name_field=None, md5_field=None, acl_field=None, size_field=None, location_field=None):
        """
        If url_prefix is given, then it will prepend to file names to get original URL,
        Otherwise, it will assume name_field contains complete URLs

        :param name_field: field name used to store file name
        :param md5_field: field name used to store original MD5
        :param size_field: field name used to store original file size
        :param url_prefix: URL prefix to prepend to all file names
        :param verify: whether or not to verify MD5 and size
        """
        super().__init__(name_field=name_field, md5_field=md5_field, size_field=size_field, acl_field=acl_field, location_field=location_field)
        if isinstance(url_prefix, str) and url_prefix:
            self.url_prefix = removeTrailingSlash(url_prefix)
        else:
            self.url_prefix = None

    def get_org_url(self):
        """
        Get file's URL in original location
        :return: URL: str
        """
        file_path = self._get_path()
        if self.url_prefix:
            return urljoin(self.url_prefix, file_path)
        else:
            return file_path

    def get_org_size(self):
        """
        Get original size if it's given in file_info, otherwise try to get it from original URL
        :return:
        """
        org_size = super().get_org_size()
        if not org_size:
            org_url = self.get_org_url()
            try:
                with requests.head(org_url) as r:
                    if not r.ok:
                        self.log.error(f'Http Error Code {r.status_code} for {org_url}')
                        return None
                    if r.headers['Content-length']:
                        return int(r.headers['Content-length'])
                    else:
                        return None
            except Exception as e:
                self.log.error(f'Could not get original size for "{org_url}"')
                self.log.debug(e)
                return None
        else:
            return org_size


