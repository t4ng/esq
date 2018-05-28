# esq
Django style Elasticsearch ORM

## Usage

```python
import esq

esq.connect(ES_HOSTS)

class User(esq.Document):
  id = esq.IntField(primary_key=True)
  name = esq.StringField()
  created_at = esq.DateTimeField()
  
# query
users = User.objects.filter(user_id__in=[1,2,3]).all()
users = User.objects.query(name__contains='aaa').filter(created_at__gt=now).all()

# logic, support and/or/invert
q = esq.Q(name='aaa') | esq.Q(name='bbb')
users = User.objects.filter(q).all()
```
