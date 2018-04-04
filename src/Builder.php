<?php

namespace CrCms\ElasticSearch;

use Elasticsearch\Client;
use Illuminate\Support\Collection;
use RuntimeException;
use stdClass;

/**
 * Class Builder
 *
 * @package CrCms\ElasticSearch
 * @author simon
 */
class Builder
{
    /**
     * @var array
     */
    public $wheres = [];

    /**
     * @var array
     */
    public $columns = [];

    /**
     * @var null
     */
    public $offset = null;

    /**
     * @var null
     */
    public $limit = null;

    /**
     * @var array
     */
    public $orders = [];

    /**
     * @var array
     */
    public $aggs = [];

    /**
     * @var string
     */
    public $index = '';

    /**
     * @var string
     */
    public $type = '';

    /**
     * @var string
     */
    public $scroll = '';

    /**
     * @var array
     */
    public $operators = [
        '=' => 'eq',
        '>' => 'gt',
        '>=' => 'gte',
        '<' => 'lt',
        '<=' => 'lte',
    ];

    /**
     * @var Grammar|null
     */
    protected $grammar = null;

    /**
     * @var \Elasticsearch\Client|null
     */
    protected $elastisearch = null;

    /**
     * @var array
     */
    protected $queryLogs = [];

    /**
     * @var bool
     */
    protected $enableQueryLog = false;

    /**
     * @var array
     */
    protected $config = [];

    /**
     * Builder constructor.
     */
    public function __construct(array $config, Grammar $grammar, Client $client)
    {
        $this->config = $config;
        $this->setGrammar($grammar);
        $this->setElasticSearch($client);
        $this->setDefault();
    }

    /**
     * @return void
     */
    protected function setDefault()
    {
        if (!empty($this->config['index'])) {
            $this->index = $this->config['index'];
        }

        if (!empty($this->config['type'])) {
            $this->type = $this->config['type'];
        }
    }

    /**
     * @param $index
     * @return Builder
     */
    public function index($index): self
    {
        $this->index = $index;

        return $this;
    }

    /**
     * @param $type
     * @return Builder
     */
    public function type($type): self
    {
        $this->type = $type;

        return $this;
    }

    /**
     * @param int $value
     * @return Builder
     */
    public function limit(int $value): self
    {
        $this->limit = $value;

        return $this;
    }

    /**
     * @param int $value
     * @return Builder
     */
    public function take(int $value): self
    {
        return $this->limit($value);
    }

    /**
     * @param int $value
     * @return Builder
     */
    public function offset(int $value): self
    {
        $this->offset = $value;

        return $this;
    }

    /**
     * @param int $value
     * @return Builder
     */
    public function skip(int $value): self
    {
        return $this->offset($value);
    }

    /**
     * @param string $field
     * @param $sort
     * @return Builder
     */
    public function orderBy(string $field, $sort): self
    {
        $this->orders[$field] = $sort;

        return $this;
    }

    /**
     * @param $field
     * @param $type
     * @return Builder
     */
    public function aggBy($field, $type): self
    {
        is_array($field) ?
            $this->aggs[] = $field :
            $this->aggs[$field] = $type;

        return $this;
    }

    /**
     * @param string $scroll
     * @return Builder
     */
    public function scroll(string $scroll): self
    {
        $this->scroll = $scroll;

        return $this;
    }

    /**
     * @param $columns
     * @return Builder
     */
    public function select($columns): self
    {
        $this->columns = is_array($columns) ? $columns : func_get_args();

        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function whereMatch($field, $value, $boolean = 'and'): self
    {
        return $this->where($field, '=', $value, 'match', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function orWhereMatch($field, $value, $boolean = 'and'): self
    {
        return $this->whereMatch($field, $value, $boolean);
    }


    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function whereTerm($field, $value, $boolean = 'and'): self
    {
        return $this->where($field, '=', $value, 'term', $boolean);
    }

    /**
     * @param $field
     * @param array $value
     * @return Builder
     */
    public function whereIn($field, array $value)
    {
        return $this->where(function (Builder $query) use ($field, $value) {
            array_map(function ($item) use ($query, $field) {
                $query->orWhereTerm($field, $item);
            }, $value);
        });
    }

    /**
     * @param $field
     * @param array $value
     * @return Builder
     */
    public function orWhereIn($field, array $value)
    {
        return $this->orWhere(function (Builder $query) use ($field, $value) {
            array_map(function ($item) use ($query, $field) {
                $query->orWhereTerm($field, $item);
            }, $value);
        });
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function orWhereTerm($field, $value, $boolean = 'or'): self
    {
        return $this->whereTerm($field, $value, $boolean);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $boolean
     * @return Builder
     */
    public function whereRange($field, $operator = null, $value = null, $boolean = 'and'): self
    {
        return $this->where($field, $operator, $value, 'range', $boolean);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return Builder
     */
    public function orWhereRange($field, $operator = null, $value = null): self
    {
        return $this->where($field, $operator, $value, 'or');
    }

    /**
     * @param $field
     * @param array $values
     * @param string $boolean
     * @return Builder
     */
    public function whereBetween($field, array $values, $boolean = 'and'): self
    {
        return $this->where($field, null, $values, 'range', $boolean);
    }

    /**
     * @param $field
     * @param array $values
     * @return Builder
     */
    public function orWhereBetween($field, array $values): self
    {
        return $this->whereBetween($field, $values, 'or');
    }

    /**
     * @param $column
     * @param null $operator
     * @param null $value
     * @param string $leaf
     * @param string $boolean
     * @return Builder
     */
    public function where($column, $operator = null, $value = null, $leaf = 'term', $boolean = 'and'): self
    {
        if ($column instanceof \Closure) {
            return $this->whereNested($column, $boolean);
        }

        if (func_num_args() === 2) {
            list($value, $operator) = [$operator, '='];
        }

        if (is_array($operator)) {
            list($value, $operator) = [$operator, null];
        }

        if ($operator !== '=') {
            $leaf = 'range';
        }

        if (is_array($value) && $leaf === 'range') {
            $value = [
                $this->operators['>='] => $value[0],
                $this->operators['<='] => $value[1],
            ];
        }

        $type = 'Basic';

        $operator = $operator ? $this->operators[$operator] : $operator;

        $this->wheres[] = compact(
            'type', 'column', 'leaf', 'value', 'boolean', 'operator'
        );

        return $this;
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $leaf
     * @return Builder
     */
    public function orWhere($field, $operator = null, $value = null, $leaf = 'term'): self
    {
        if (func_num_args() === 2) {
            list($value, $operator) = [$operator, '='];
        }

        return $this->where($field, $operator, $value, $leaf, 'or');
    }

    /**
     * @param \Closure $callback
     * @param $boolean
     * @return Builder
     */
    public function whereNested(\Closure $callback, $boolean): self
    {
        $query = $this->newQuery();

        call_user_func($callback, $query);

        return $this->addNestedWhereQuery($query, $boolean);
    }

    /**
     * @return static
     */
    public function newQuery(): self
    {
        return new static($this->config, $this->grammar, $this->elastisearch);
    }

    /**
     * @return stdClass|null
     */
    public function first()
    {
        $this->limit = 1;

        $results = $this->runQuery($this->grammar->compileSelect($this));

        return $this->metaData($results)->first();
    }

    /**
     * @return Collection
     */
    public function get(): Collection
    {
        $results = $this->runQuery($this->grammar->compileSelect($this));

        return $this->metaData($results);
    }

    /**
     * @param int $page
     * @param int $perPage
     * @return Collection
     */
    public function paginate(int $page, int $perPage = 15): Collection
    {
        $from = (($page * $perPage) - $perPage);

        if (empty($this->offset)) {
            $this->offset = $from;
        }

        if (empty($this->limit)) {
            $this->limit = $perPage;
        }

        $results = $this->runQuery($this->grammar->compileSelect($this));

        $data = collect($results['hits']['hits'])->map(function ($hit) {
            return (object)array_merge($hit['_source'], ['_id' => $hit['_id']]);
        });

        $maxPage = intval(ceil($results['hits']['total'] / $perPage));
        return collect([
            'total' => $results['hits']['total'],
            'per_page' => $perPage,
            'current_page' => $page,
            'next_page' => $page < $maxPage ? $page + 1 : $maxPage,
            //'last_page' => $maxPage,
            'total_pages' => $maxPage,
            'from' => $from,
            'to' => $from + $perPage,
            'data' => $data
        ]);
    }

    /**
     * @param $id
     * @return null|object
     */
    public function byId($id)
    {
        //$query = $this->newQuery();

        $result = $this->runQuery(
            $this->whereTerm('_id', $id)->getGrammar()->compileSelect($this)
        );

        return isset($result['hits']['hits'][0]) ?
            $this->sourceToObject($result['hits']['hits'][0]) :
            null;
    }

    /**
     * @param $id
     * @return stdClass
     */
    public function byIdOrFail($id): stdClass
    {
        $result = $this->byId($id);

        if (empty($result)) {
            throw new RuntimeException('Resource not found');
        }

        return $result;
    }

    /**
     * @param callable $callback
     * @param int $limit
     * @param string $scroll
     * @return bool
     */
    public function chunk(callable $callback, $limit = 2000, $scroll = '10m')
    {
        if (empty($this->scroll)) {
            $this->scroll = $scroll;
        }

        if (empty($this->limit)) {
            $this->limit = $limit;
        }

        $results = $this->runQuery($this->grammar->compileSelect($this), 'search');

        if ($results['hits']['total'] === 0) {
            return null;
        }

        $total = $this->limit;
        $whileNum = intval(floor($results['hits']['total'] / $this->limit));

        do {
            if (call_user_func($callback, $this->metaData($results)) === false) {
                return false;
            }

            $results = $this->runQuery(['scroll_id' => $results['_scroll_id'], 'scroll' => $this->scroll], 'scroll');

            $total += count($results['hits']['hits']);
        } while ($whileNum--);
    }

    /**
     * @param array $data
     * @param null $id
     * @param string $key
     * @return stdClass
     */
    public function create(array $data, $id = null, $key = 'id'): stdClass
    {
        $id = $id ? $id : isset($data[$key]) ? $data[$key] : uniqid();

        $result = $this->runQuery(
            $this->grammar->compileCreate($this, $id, $data),
            'create'
        );

        if (!isset($result['result']) || $result['result'] !== 'created') {
            throw new RunTimeException('Create params: ' . json_encode($this->getLastQueryLog()));
        }

        $data['_id'] = $id;
        return (object)$data;
    }

    /**
     * @param $id
     * @param array $data
     * @return bool
     */
    public function update($id, array $data): bool
    {
        $result = $this->runQuery($this->grammar->compileUpdate($this, $id, $data), 'update');

        if (!isset($result['result']) || $result['result'] !== 'updated') {
            throw new RunTimeException('Update error params: ' . json_encode($this->getLastQueryLog()));
        }

        return true;
    }

    /**
     * @param $id
     * @return bool
     */
    public function delete($id)
    {
        $result = $this->runQuery($this->grammar->compileDelete($this, $id), 'delete');

        if (!isset($result['result']) || $result['result'] !== 'deleted') {
            throw new RunTimeException('Delete error params:' . json_encode($this->getLastQueryLog()));
        }

        return true;
    }

    /**
     * @return int
     */
    public function count(): int
    {
        $result = $this->runQuery($this->grammar->compileSelect($this), 'count');
        return $result['count'];
    }

    /**
     * @return Grammar|null
     */
    public function getGrammar()
    {
        return $this->grammar;
    }

    /**
     * @param Grammar $grammar
     * @return $this
     */
    public function setGrammar(Grammar $grammar)
    {
        $this->grammar = $grammar;

        return $this;
    }

    /**
     * @param Client $client
     * @return $this
     */
    public function setElasticSearch(Client $client)
    {
        $this->elastisearch = $client;

        return $this;
    }

    /**
     * @return Client|null
     */
    public function getElasticSearch()
    {
        return $this->elastisearch;
    }

    /**
     * @return Builder
     */
    public function enableQueryLog(): self
    {
        $this->enableQueryLog = true;

        return $this;
    }

    /**
     * @return Builder
     */
    public function disableQueryLog(): self
    {
        $this->enableQueryLog = false;

        return $this;
    }

    /**
     * @return array
     */
    public function getQueryLog(): array
    {
        return $this->queryLogs;
    }

    /**
     * @return array
     */
    public function getLastQueryLog(): array
    {
        return empty($this->queryLogs) ? '' : end($this->queryLogs);
    }

    /**
     * @return \Elasticsearch\Client
     */
    public function search()
    {
        return $this->elastisearch;
    }

    /**
     * @param array $params
     * @param string $method
     * @return mixed
     */
    protected function runQuery(array $params, string $method = 'search')
    {
        if ($this->enableQueryLog) {
            $this->queryLogs[] = $params;
        }

        return call_user_func([$this->elastisearch, $method], $params);
    }

    /**
     * @param array $results
     * @return Collection
     */
    protected function metaData(array $results): Collection
    {
        return collect($results['hits']['hits'])->map(function ($hit) {
            return $this->sourceToObject($hit);
        });
    }

    /**
     * @param array $result
     * @return object
     */
    protected function sourceToObject(array $result): stdClass
    {
        return (object)array_merge($result['_source'], ['_id' => $result['_id']]);
    }

    /**
     * @param $query
     * @param string $boolean
     * @return Builder
     */
    protected function addNestedWhereQuery($query, $boolean = 'and'): self
    {
        if (count($query->wheres)) {
            $type = 'Nested';
            $this->wheres[] = compact('type', 'query', 'boolean');
        }

        return $this;
    }
}