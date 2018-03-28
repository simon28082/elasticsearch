# Crcms Elasticsearch

## Version Matrix

| Elasticsearch Version | crcms/elasticsearch Branch |
| --------------------- | ------------------------ |
| >= 6.0                | 1.*                      |
| >= 5.0, < 6.0         | 0.*                      |

## Install

You can install the package via composer:

```
composer require crcms/elasticsearch
```

## Laravel

Modify ``config / app.php``

```
'providers' => [
    CrCms\ElasticSearch\LaravelServiceProvider::class,
]

```

If you'd like to make configuration changes in the configuration file you can pubish it with the following Aritsan command:
```
php artisan vendor:publish --provider="CrCms\ElasticSearch\LaravelServiceProvider"
```



Quickstart
----


### Index a document

In elasticsearch-php, almost everything is configured by associative arrays.  The REST endpoint, document and optional parameters - everything is an associative array.

To index a document, we need to specify four pieces of information: index, type, id and a document body. This is done by
constructing an associative array of key:value pairs.  The request body is itself an associative array with key:value pairs
corresponding to the data in your document:

### Create

```php

Route::get('test/create',function(\CrCms\ElasticSearch\Builder $builder){
    $result = $builder->index('index')->type('type')->create([
		'key' => 'value',
    ]);
    dump($result);
});

```

### Update

```php

Route::get('test/create',function(\CrCms\ElasticSearch\Builder $builder){
    $result = $builder->index('index')->type('type')->update('id',[
		'key' => 'value2',
    ]);
    dump($result);
});

```

### Delete

```php

Route::get('test/create',function(\CrCms\ElasticSearch\Builder $builder){
    $result = $builder->index('index')->type('type')->delete('id');
    dump($result);
});

```

### Select

```php

Route::get('test/create',function(\CrCms\ElasticSearch\Builder $builder){
    $builder = $builder->index('index')->type('type');
	
	//SQL:select ... where id = 1 limit 1;
	$result = $builder->whereTerm('id',1)->first();
	
	//SQL:select ... where (key=1 or key=2) and key1=1
	$result = $builder->where(function (Builder $inQuery) {
		$inQuery->whereTerm('key',1)->orWhereTerm('key',2)
	})->whereTerm('key1',1)->get();
	
});

```

## License
[MIT license](https://opensource.org/licenses/MIT)