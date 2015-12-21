import sys
import datetime

from dateutil import parser as date_parser
from elasticsearch import Elasticsearch

_all_es = {}

_Q_OP_MAP = {
    'exact': 'term',
    'in': 'terms',
    'contains': 'match',
}

PY3 = sys.version_info[0] == 3

if PY3:
    long = int


def with_metaclass(meta, base=object):
    return meta('NewBase', (base,), {})


def connect(hosts, alias='default'):
    hosts = [hosts] if not isinstance(hosts, list) else hosts
    _all_es[alias] = Elasticsearch(hosts=hosts)


class Q(object):
    def __init__(self, *args, **kwargs):
        self._logic = ''
        self._list = [q.to_dict() for q in args]

        for name, value in kwargs.items():
            ss = name.split('__')
            name = ss[0]
            op = ss[1] if len(ss) == 2 else 'exact'

            if name == 'id' and op == 'in':
                op = 'ids'
                name = 'values'
            elif op in {'gt', 'gte', 'lt', 'lte'}:
                op = 'range'
                value = {op: value}

            self._list.append({_Q_OP_MAP.get(op, op): {name: value}})

        self._logic = 'must'

    def empty(self):
        return len(self._list) == 0

    def merge(self, t, logic):
        assert isinstance(t, Q)

        if len(self._list) == 1 or self._logic == logic:
            if len(t._list) == 1 or t._logic == logic:
                q = self.__class__()
                q._list = self._list + t._list
            else:
                q = self.clone()
                q._list.append(t.to_dict())

        elif len(t._list) == 1 or t._logic == logic:
            q = t.clone()
            q._list.insert(0, self.to_dict())

        else:
            q = self.__class__()
            q._list = [self.to_dict() + t.to_dict()]

        q._logic = logic
        return q

    def __or__(self, t):
        return self.merge(t, 'should')

    def __and__(self, t):
        return self.merge(t, 'must')

    def __not__(self):
        if len(self._list) == 1:
            q = self.clone()
            q._logic = 'must_not'
            return q

        q = self.__class__()
        q._logic = 'must_not'
        q._list.append(self.to_dict())
        return q

    def to_dict(self):
        return {'bool': {self._logic: [i for i in self._list]}}

    @classmethod
    def from_dict(cls, d):
        q = cls()
        for logic, l in d['bool'].items():
            q._logic = logic
            q._list = l
        return q

    def clone(self):
        return self.from_dict(self.to_dict())


class QuerySet(object):
    def __init__(self, owner):
        self._index = owner._meta['index']
        self._doc_type = owner._meta['doc_type']
        self._owner = owner
        self._es = owner.get_es()

        self.fq = Q()
        self.qq = Q()
        self._extra_body = {}
        self._params = {}

        self._count = None

    def clone(self):
        qs = self.__class__(self._owner)
        qs.fq = self.fq.clone()
        qs.qq = self.qq.clone()
        qs._extra_body = self._extra_body.copy()
        qs._params = self._params.copy()
        return qs

    def filter(self, *args, **kwargs):
        qs = self.clone()
        qs.fq = qs.fq & Q(*args, **kwargs)
        return qs

    def query(self, *args, **kwargs):
        qs = self.clone()
        qs.qq = qs.qq & Q(*args, **kwargs)
        return qs

    def params(self, **kwargs):
        qs = self.clone()
        qs._params.update(kwargs)
        return qs

    def routing(self, key):
        return self.params(routing=key)

    def extra(self, **kwargs):
        qs = self.clone()
        qs._extra_body.update(kwargs)
        return qs

    def skip(self, n):
        return self.extra(**{'from': n})

    def limit(self, n):
        return self.extra(size=n)

    def aggs(self, **kwargs):
        return self.extra(aggs=kwargs)

    def order_by(self, *keys):
        sl = []
        for key in keys:
            if isinstance(key, str) and key.startswith('-'):
                key = {key[1:]: {'order': 'desc'}}
            sl.append(key)

        qs = self.clone()
        qs._extra_body['sort'] = qs._extra_body.get('sort', []) + sl
        return qs

    def to_dict(self):
        body = {
            'query': {
                'filtered': {
                    'query': self.qq.to_dict(),
                    'filter': self.fq.to_dict(),
                },
            },
        }
        body.update(self._extra_body)
        return body

    def count(self):
        if self._count is None:
            self._count = self._es.count(index=self._index,
                                          doc_type=self._doc_type,
                                          body=self.to_dict(),
                                          **self._params)['count']

        return self._count

    def execute(self):
        result = self._es.search(index=self._index,
                                 doc_type=self._doc_type,
                                 body=self.to_dict(),
                                 **self._params)
        return result

    def all(self):
        result = self.execute()
        result = result['hits']
        self._count = result['total']
        return [self._owner.from_dict(hit) for hit in result['hits']]

    def first(self):
        r = self.limit(1).all()
        return r[0] if r else None

    def __len__(self):
        return self.count()

    def __getitem__(self, k):
        if isinstance(k, slice):
            offset = k.start or 0
            count = (k.stop - offset) if k.stop else 0
            assert offset >= 0 and count >= 0

            qs = self.clone()
            if offset:
                qs = qs.skip(offset)
            if count:
                qs = qs.skip(count)

            return qs.all()
        elif isinstance(k, int):
            assert k >= 0
            return self.skip(k).first()
        else:
            raise IndexError 


class QuerySetDescriptor(object):
    def __get__(self, instance, owner):
        if not hasattr(owner, '_objects'):
            owner._objects = QuerySet(owner)

        return owner._objects


class Field(object):
    type = object

    def __init__(self, required=False, primary_key=False, routing=False, **kwargs):
        self.required = required
        self.primary_key = primary_key
        self.routing = routing
        self._attrs = kwargs

    def serialize(self, value):
        return value

    def unserialize(self, value):
        return value


class StringField(Field):
    type = str


class IntField(Field):
    type = int


class LongField(Field):
    type = (int, long)


class DateTimeField(Field):
    type = datetime.datetime

    def unserialize(self, value):
        return date_parser.parse(value)


class FieldDescriptor(object):
    def __init__(self, name, field):
        self._name = name
        self._field = field

    def __get__(self, instance, owner):
        return instance._data.get(self._name)

    def __set__(self, instance, value):
        instance._data[self._name] = value


class DocumentMetaClass(type):
    def __new__(cls, name, bases, attrs):
        fields = {}
        meta = attrs.pop('meta', {})

        for name, value in list(attrs.items()):
            if isinstance(value, Field):
                fields[name] = value
                attrs[name] = FieldDescriptor(name, value)

                if value.primary_key:
                    meta['primary_key'] = name

                if value.routing:
                    meta['routing'] = name

            elif name == 'Meta':
                meta.update(value.__dict__)

        attrs['_fields'] = fields
        attrs['_meta'] = meta
        return type.__new__(cls, name, bases, attrs)


DOC_META_FIELDS = ('id', 'parent', 'routing', 'timestamp', 'ttl', 'version', 'version_type')


class Document(with_metaclass(DocumentMetaClass)):
    objects = QuerySetDescriptor()

    def __init__(self, **kwargs):
        self._doc_meta = kwargs.pop('meta', {})
        self._data = {k: v for k, v in kwargs.items() if k in self._fields}

    @classmethod
    def get_es(cls):
        alias = cls._meta.get('alias', 'default')
        return _all_es[alias]

    def to_dict(self):
        d = self._doc_meta.copy()
        d['_source'] = self._data
        return d

    @classmethod
    def from_dict(cls, d):
        data = d.pop('_source')
        for name, value in list(data.items()):
            field = cls._fields.get(name)
            if not field:
                continue
            data[name] = field.unserialize(value)

        return cls(meta=d, **data)

    @classmethod
    def get(cls, id, **kwargs):
        result = cls.get_es().get(
            index=cls._meta['index'],
            doc_type=cls._meta['doc_type'],
            id=id,
            ignore=404,
            **kwargs
        )

        return cls.from_dict(result) if result['found'] else None

    @property
    def doc_meta(self):
        doc_meta = {k.lstrip('_'): v for k, v in self._doc_meta.items()
                    if k.lstrip('_') in DOC_META_FIELDS}

        if 'id' not in doc_meta:
            id = self._data.get(self._meta.get('primary_key'))
            if id:
                doc_meta['id'] = id

        if 'routing' not in doc_meta:
            routing = self._data.get(self._meta.get('routing'))
            if routing:
                doc_meta['routing'] = routing

        return doc_meta

    def validate(self):
        for name, field in self._fields.items():
            value = self._data.get(name)
            if value is None:
                if field.required:
                    raise AttributeError('%s field is required' % name)
                continue

            values = value if isinstance(value, list) else [value]
            for v in values:
                if not isinstance(v, field.type):
                    raise TypeError('%s type error' % name)

    @classmethod
    def serialize(cls, data):
        serialized_data = {}
        for name, value in data.items():
            field = cls._fields.get(name)
            value = field.serialize(value) if field else value
            serialized_data[name] = value

        return serialized_data

    def update(self, **kwargs):
        self._data.update(kwargs)
        kwargs = self.serialize(kwargs)

        result = self.get_es().update(
            index=self._meta['index'],
            doc_type=self._meta['doc_type'],
            body={'doc': kwargs},
            **self.doc_meta
        )

        result.pop('_source', None)
        self._doc_meta.update(result)
        return result

    def save(self, **kwargs):
        if kwargs.pop('validate', True):
            self.validate()

        kwargs.update(self.doc_meta)
        result = self.get_es().index(
            index=self._meta['index'],
            doc_type=self._meta['doc_type'],
            body=self.serialize(self._data),
            **kwargs
        )

        result.pop('_source', None)
        self._doc_meta.update(result)
        return result['created']

    def delete(self, **kwargs):
        kwargs.update(self.doc_meta)
        result = self.get_es().delete(
            index=self._meta['index'],
            doc_type=self._meta['doc_type'],
            **kwargs
        )

        return result
