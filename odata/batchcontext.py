# -*- coding: utf-8 -*-

from odata.query import Query
from odata.context import Context
from odata.exceptions import ODataError
from odata.action import Action, Function
from uuid import uuid4 as uuid
from odata.changeset import ChangeSet, Change, ChangeAction, ActionChange, FunctionChange

class BatchContext(Context):
    def __init__(self, service, session=None, auth=None):
        super().__init__(session=session, auth=auth)
        self.boundary = 'batch_%s' % (uuid())
        self.batch = True
        self._parts = []
        self._changeset = None
        self._content_id_to_entity_map = []
        self._service = service

    def open_changeset(self):
        if self._changeset is not None:
            raise Exception('Close the current change set first before opening a new one. They cannot be nested.')
        self._changeset = ChangeSet()
        self._parts.append(self._changeset)

    def close_changeset(self):
        if self._changeset is None:
            raise Exception('Open a change set first before closing one.')
        self._changeset = None

    def reset(self):
        self._parts = []
        self._content_id_to_entity_map = []
        self._changeset = None

    def execute(self):
        if self._changeset is not None:
            raise Exception('Call close_changeset before executing the batch request')
        
        content_id_to_entity_map = self._content_id_to_entity_map.copy() # store for later use
        pl = self._get_payload()

        self.reset() # reset know in case something fails. Prevent unknown state

        url = self._service.url + '$batch'
        
        headers = self.connection.base_headers.copy()
        headers.update({
            'Content-Type': 'multipart/mixed;boundary=%s;charset=utf-8' % (self.boundary),
        })
        response = self.connection.execute_post_raw(
            url,
            headers,
            pl,
        )

        # print(response) # TODO: remove print

        post_processed = self._apply_response_to_entities(response, content_id_to_entity_map)
        return {
            'entities': post_processed['entities'],
            'responses': post_processed['response_map'],
            'response_raw': response,
            'id_to_entity': content_id_to_entity_map,
        }

    def _apply_response_to_entities(self, response, content_id_to_entity_map):
        m = content_id_to_entity_map
        entities = []
        response_map = []
        processed_content_ids = []
        for entity, content_id in m:
            saved_data = {}
            error_msg = None
            error_code = None

            resp_for_entity = [x for x in response['responses'] if x['id'] == content_id]
            if resp_for_entity is None or len(resp_for_entity) != 1:
                error_code = 500
                error_msg = 'Server sent no error message. There might be errors in previous operations of the same batch.'
            else:
                processed_content_ids.append(content_id)
                resp_for_entity = resp_for_entity[0]
            
                if resp_for_entity['status'] < 200 or resp_for_entity['status'] >= 300:
                    error_code = resp_for_entity['status']
                    error_msg = "HTTP %s for changeset '%s' and content_id '%s' with error %s" % (
                        resp_for_entity['status'],
                        resp_for_entity['atomicityGroup'],
                        resp_for_entity['id'],
                        resp_for_entity.get('body', {}).get('error', {}).get('message', 'Server sent no error message')
                    )

            if error_msg is None:
                saved_data = resp_for_entity['body']
                for k in list(saved_data.keys()):
                    # remove odata annotations in the response
                    if k.startswith('@odata.'):
                        saved_data.pop(k)
                
                entity.__odata__.reset() # reset dirty flags etc
                entity.__odata__.update(saved_data)
                
                response_map.append((entity, resp_for_entity['status'], None))
            else:
                response_map.append((entity, error_code, error_msg))

            entities.append(entity)

        for res in [x for x in response['responses'] if x['id'] not in processed_content_ids]:
            if res['status'] < 200 or res['status'] >= 300:
                error_code = res['status']
                error_msg = "HTTP %s for content_id '%s' with error %s" % (
                    res['status'],
                    res['id'],
                    res.get('body', {}).get('error', {}).get('message', 'Server sent no error message')
                )
                response_map.append((None, error_code, error_msg))
            else:
                response_map.append((None, res['status'], None))

        return {
            'entities': entities,
            'response_map': response_map,
        }

    def _get_payload(self):
        parts_str = [
          '--%s' % (self.boundary),
        ]
        for part in self._parts:
            pl = part.get_payload()
            parts_str.append(pl)

        parts_str.append('--%s--' % self.boundary)
        return '\n'.join(parts_str).replace('\n', '\r\n').encode('utf-8')


    def query(self, entitycls):
        raise NotImplementedError('calling an action/function in a batch operation is not implemented')
        # if self._changeset is not None:
        #   raise Exception('Cannot read data within a change set. Call close_changeset first')
        # q = Query(entitycls, connection=self.connection)
        # return q

    def call(self, action_or_function, callback=None, **parameters):
        if self._changeset is None:
            raise Exception('Call open_changeset before doing data modification requests')
        if isinstance(action_or_function, Action):
            change = ActionChange(action_or_function, **parameters)
            self._changeset.add_change(change, callback=callback)
            return
        elif isinstance(action_or_function, Function):
            change = FunctionChange(action_or_function, **parameters)
            self._changeset.add_change(change, callback=callback)
            return

    def call_with_query(self, action_or_function, query, **parameters):
        raise NotImplementedError('calling an action/function with query in a batch operation is not implemented')

    def save(self, entity, force_refresh=True, parent_resource=None) -> str:
        """
        Creates a POST or PATCH call to the service. If the entity already has
        a primary key, an update is called. Otherwise the entity is inserted
        as new. Updating an entity will only send the changed values

        :param entity: Model instance to insert or update
        :type entity: EntityBase
        :param force_refresh: Read full entity data again from service after PATCH call
        :raises ODataConnectionError: Invalid data or serverside error. Server returned an HTTP error code
        """

        if self.is_entity_saved(entity):
            if parent_resource is not None:
                raise ValueError((
                    "Cannot provide a parent_resource for a non-insert operation for entity %s. "
                    "This feature is only used to reference entities "
                    "that are created in the same batch request."
                ) % (entity.__repr__))
            return self._update_existing(entity, force_refresh=force_refresh)
        else:
            return self._insert_new(entity, parent_resource=parent_resource)

    def delete(self, entity):
        """
        Creates a DELETE call to the service, deleting the entity

        :type entity: EntityBase
        :raises ODataConnectionError: Delete not allowed or a serverside error. Server returned an HTTP error code
        """
        if self._changeset is None:
            raise Exception('Call open_changeset before doing data modification requests')

        raise Exception('Delete still needs to be implemented')
        # TODO:
      
        # self.log.info(u'Deleting entity: {0}'.format(entity))
        # # url = entity.__odata__.instance_url
        # url = entity.__odata__.instance_url[len(self._service.url) - 1:]
        # self.connection.execute_delete(url)
        # entity.__odata__.persisted = False
        # self.log.info(u'Success')

    def _insert_new(self, entity, parent_resource=None):
        """
        Creates a POST call to the service, sending the complete new entity

        :type entity: EntityBase
        :type parent_resource: EntityBase another entity that is also created in the same changeset and that
           we want to reference (e.g. create an Author first and then create some books from this author. Author would be the parent.)
        """
        if self._changeset is None:
            raise Exception('Call open_changeset before doing data modification requests')

        es = entity.__odata__
        insert_data = es.data_for_insert()
        
        if parent_resource is None:
            url = entity.__odata_url__()[len(self._service.url) - 1:]
        else:
            es_p = parent_resource.__odata__
            entity_type = entity.__odata_schema__['type']
            parent_entity_type = parent_resource.__odata_schema__['type']

            parent_nav_prop = [x for x in es_p.navigation_properties if x[1].navigated_property_type == entity_type][0][1]

            content_id = [x for x in self._content_id_to_entity_map if x[0] is parent_resource][0][1]
            # use $<Content-ID>/navProperty as url
            url = '$%s/%s' % (content_id, parent_nav_prop.name)

            # via the url we tell odata that we want to create a sub-entity (e.g. Author = parent and Book = sub).
            # In case the book has a reference to author (e.g. author_ID) we need to remove it as it has no value and
            # defaults to a "null"-value if not set. However, we just dont want to send any value (not even null) for this field
            nav_prop = [x for x in es.navigation_properties if x[1].navigated_property_type == parent_entity_type]
            if nav_prop and len(nav_prop) > 0:
                fk = nav_prop[0][1].foreign_key
                if fk is not None and fk in insert_data:
                    # remove if it exists in the dict
                    insert_data.pop(fk)

        if url is None:
            msg = 'Cannot insert Entity that does not belong to EntitySet: {0}'.format(entity)
            raise ODataError(msg)    

        def cb(self, saved_data):
            es.reset()
            es.connection = self.connection
            es.persisted = True
            if saved_data is not None:
                es.update(saved_data)
            self.log.info(u'Success')

        content_id = self._changeset.add_change(Change(
          url,
          insert_data,
          ChangeAction.CREATE,
        ), callback=cb)

        self._content_id_to_entity_map.append((entity, content_id))
        return content_id

    def _update_existing(self, entity, force_refresh=True):
        """
        Creates a PATCH call to the service, sending only the modified values

        :type entity: EntityBase
        """
        if self._changeset is None:
            raise Exception('Call open_changeset before doing data modification requests')
        
        es = entity.__odata__
        if es.instance_url is None:
            msg = 'Cannot update Entity that does not belong to EntitySet: {0}'.format(entity)
            raise ODataError(msg)

        patch_data = es.data_for_update()

        if len([i for i in patch_data if not i.startswith('@')]) == 0:
            self.log.debug(u'Nothing to update: {0}'.format(entity))
            return

        # url = es.instance_url

        url = es.instance_url[len(self._service.url) - 1:]

        def cb(self, saved_data):
            es.reset()
            if saved_data is not None and force_refresh:
                saved_data = self.connection.execute_get(url)
            if saved_data is not None:
                entity.__odata__.update(saved_data)
            self.log.info(u'Success')

        content_id = self._changeset.add_change(Change(
          url,
          patch_data,
          ChangeAction.UPDATE,
        ), callback=cb)

        self._content_id_to_entity_map.append((entity, content_id))
