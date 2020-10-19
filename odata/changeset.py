# -*- coding: utf-8 -*-

import json
import enum
from uuid import uuid4 as uuid
import socket

class ChangeAction:
    CREATE = 'POST'
    UPDATE = 'PATCH'
    DELETE = 'DELETE'

class Change:
    def __init__(self, url: str, data, action: ChangeAction):
        self.content_id = None
        self.base_headers = {
          'Content-Type': 'application/http;charset=utf-8',
          'Content-Transfer-Encoding': 'binary',
        }
        self.data = data
        self.method = action
        self.url = url

    def get_content_id(self):
        return self.content_id

    def set_content_id(self, content_id: str):
        self.content_id = content_id
        return self

    def get_payload(self):
        headers = self.base_headers.copy()
        headers.update({
          'Content-ID': self.content_id,
        })

        parts = []

        for key, value in headers.items():
            parts.append('%s: %s' % (key, value))
        parts.append('')

        parts.append('%s %s HTTP/1.1' % (self.method, self.url))
        parts.append('Host: %s' % socket.gethostname())
        parts.append('Content-Type: application/json;type=entry;charset=utf-8')
        parts.append('')
        parts.append(json.dumps(self.data, indent=2, ensure_ascii=False))

        return '\n'.join(parts)


class ChangeSet:
    def __init__(self):
        self.boundary = 'changeset_%s' % (uuid())
        self._changes = []
        self._callbacks = []

    def add_change(self, change: Change, callback=None) -> str:
        self._changes.append(change)
        self._callbacks.append(callback)
        
        change_content_id = '%s-%s' % (self.boundary, len(self._changes))
        change.set_content_id(change_content_id)
        return change_content_id

    def get_boundary(self):
        return self.boundary

    def get_payload(self):
        parts = [
          'Content-Type: multipart/mixed;boundary=%s;charset=utf-8' % (self.get_boundary()),
          '',
        ]

        for change in self._changes:
          parts.append('--%s' % (self.get_boundary()))
          parts.append(change.get_payload())

        parts.append('--%s--' % (self.get_boundary()))

        return '\n'.join(parts)
