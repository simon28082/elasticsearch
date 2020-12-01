<?php

declare(strict_types=1);

namespace CrCms\ElasticSearch;

use Closure;
use Elasticsearch\Client;

class Query
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
     * @var int
     */
    public $offset;

    /**
     * @var int
     */
    public $limit;

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
    public $index;

    /**
     * @var string
     */
    public $type;

    /**
     * @var string
     */
    public $scroll;

    /**
     * @var array
     */
    public $operators = [
        '='  => 'eq',
        '>'  => 'gt',
        '>=' => 'gte',
        '<'  => 'lt',
        '<=' => 'lte',
        '!=' => 'ne',
    ];

    /**
     * @var Grammar
     */
    protected $grammar;

    /**
     * @var Client
     */
    protected $elasticsearch;

    /**
     * @var array
     */
    protected $config;

    /**
     * @param Grammar $grammar
     * @param Client  $client
     */
    public function __construct(Grammar $grammar, Client $client)
    {
        $this->setGrammar($grammar);
        $this->setElasticSearch($client);
    }

    /**
     * @param string|array $index
     *
     * @return Query
     */
    public function index($index): self
    {
        $this->index = $index;

        return $this;
    }

    /**
     * @param string $type
     *
     * @return Query
     */
    public function type($type): self
    {
        $this->type = $type;

        return $this;
    }

    /**
     * @param int $value
     *
     * @return Query
     */
    public function limit(int $value): self
    {
        $this->limit = $value;

        return $this;
    }

    /**
     * @param int $value
     *
     * @return Query
     */
    public function take(int $value): self
    {
        return $this->limit($value);
    }

    /**
     * @param int $value
     *
     * @return Query
     */
    public function offset(int $value): self
    {
        $this->offset = $value;

        return $this;
    }

    /**
     * @param int $value
     *
     * @return Query
     */
    public function skip(int $value): self
    {
        return $this->offset($value);
    }

    /**
     * @param string $field
     * @param $sort
     *
     * @return Query
     */
    public function orderBy(string $field, $sort): self
    {
        $this->orders[$field] = $sort;

        return $this;
    }

    /**
     * @param string|array $field
     * @param $type
     *
     * @return Query
     */
    public function aggBy($field, $type = null): self
    {
        is_array($field) ?
            $this->aggs[] = $field :
            $this->aggs[$field] = $type;

        return $this;
    }

    /**
     * @param string $scroll
     *
     * @return Query
     */
    public function scroll(string $scroll): self
    {
        $this->scroll = $scroll;

        return $this;
    }

    /**
     * @param string|array $columns
     *
     * @return Query
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
     *
     * @return Query
     */
    public function whereMatch($field, $value, $boolean = 'and'): self
    {
        return $this->where($field, '=', $value, 'match', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     *
     * @return Query
     */
    public function orWhereMatch($field, $value, $boolean = 'or'): self
    {
        return $this->whereMatch($field, $value, $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     *
     * @return Query
     */
    public function whereTerm($field, $value, $boolean = 'and'): self
    {
        return $this->where($field, '=', $value, 'term', $boolean);
    }

    /**
     * @param $field
     * @param array $value
     *
     * @return Query
     */
    public function whereIn($field, array $value): self
    {
        return $this->where(function (self $query) use ($field, $value) {
            array_map(function ($item) use ($query, $field) {
                $query->orWhereTerm($field, $item);
            }, $value);
        });
    }

    /**
     * @param $field
     * @param array $value
     *
     * @return Query
     */
    public function orWhereIn($field, array $value): self
    {
        return $this->orWhere(function (self $query) use ($field, $value) {
            array_map(function ($item) use ($query, $field) {
                $query->orWhereTerm($field, $item);
            }, $value);
        });
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     *
     * @return Query
     */
    public function orWhereTerm($field, $value, $boolean = 'or'): self
    {
        return $this->whereTerm($field, $value, $boolean);
    }

    /**
     * @param $field
     * @param null   $operator
     * @param null   $value
     * @param string $boolean
     *
     * @return Query
     */
    public function whereRange($field, $operator = null, $value = null, $boolean = 'and'): self
    {
        return $this->where($field, $operator, $value, 'range', $boolean);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     *
     * @return Query
     */
    public function orWhereRange($field, $operator = null, $value = null): self
    {
        return $this->where($field, $operator, $value, 'or');
    }

    /**
     * @param $field
     * @param array  $values
     * @param string $boolean
     *
     * @return Query
     */
    public function whereBetween($field, array $values, $boolean = 'and'): self
    {
        return $this->where($field, null, $values, 'range', $boolean);
    }

    /**
     * @param $field
     * @param array $values
     *
     * @return Query
     */
    public function orWhereBetween($field, array $values): self
    {
        return $this->whereBetween($field, $values, 'or');
    }

    /**
     * @param $field
     * @param array  $values
     * @param string $boolean
     *
     * @return Query
     */
    public function whereNotBetween($field, array $values, $boolean = 'and'): self
    {
        return $this->where($field, '!=', $values, 'range', $boolean);
    }

    /**
     * @param $field
     * @param array $values
     *
     * @return Builder
     */
    public function orWhereNotBetween($field, array $values): self
    {
        return $this->whereNotBetween($field, $values, 'or');
    }

    /**
     * @param $field
     * @param string $boolean
     *
     * @return Query
     */
    public function whereExists($field, $boolean = 'and'): self
    {
        return $this->where($field, '=', '', 'exists', $boolean);
    }

    /**
     * @param $field
     * @param string $boolean
     *
     * @return Query
     */
    public function whereNotExists($field, $boolean = 'and'): self
    {
        return $this->where($field, '!=', '', 'exists', $boolean);
    }

    /**
     * @param Closure|string $column
     * @param string|null    $operator
     * @param string|null    $value
     * @param string         $leaf
     * @param string         $boolean
     *
     * @return Query
     */
    public function where($column, $operator = null, $value = null, string $leaf = 'term', string $boolean = 'and'): self
    {
        if ($column instanceof Closure) {
            return $this->whereNested($column, $boolean);
        }

        if (func_num_args() === 2) {
            list($value, $operator) = [$operator, '='];
        }

        if (is_array($operator)) {
            list($value, $operator) = [$operator, null];
        }

        if (in_array($operator, ['>=', '>', '<=', '<'])) {
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
            'type',
            'column',
            'leaf',
            'value',
            'boolean',
            'operator'
        );

        return $this;
    }

    /**
     * @param $field
     * @param null   $operator
     * @param null   $value
     * @param string $leaf
     *
     * @return Query
     */
    public function orWhere($field, $operator = null, $value = null, $leaf = 'term'): self
    {
        if (func_num_args() === 2) {
            list($value, $operator) = [$operator, '='];
        }

        return $this->where($field, $operator, $value, $leaf, 'or');
    }

    /**
     * @param Closure $callback
     * @param string  $boolean
     *
     * @return Query
     */
    public function whereNested(Closure $callback, string $boolean): self
    {
        $query = $this->newQuery();

        call_user_func($callback, $query);

        return $this->addNestedWhereQuery($query, $boolean);
    }

    /**
     * @return Query
     */
    public function newQuery(): self
    {
        return new static($this->grammar, $this->elasticsearch);
    }

    /**
     * @return Grammar
     */
    public function getGrammar(): Grammar
    {
        return $this->grammar;
    }

    /**
     * @param Grammar $grammar
     *
     * @return $this
     */
    public function setGrammar(Grammar $grammar)
    {
        $this->grammar = $grammar;

        return $this;
    }

    /**
     * @param Client $client
     *
     * @return $this
     */
    public function setElasticSearch(Client $client)
    {
        $this->elasticsearch = $client;

        return $this;
    }

    /**
     * @return Client
     */
    public function getElasticSearch(): Client
    {
        return $this->elasticsearch;
    }

    /**
     * @return Client
     */
    public function search(): Client
    {
        return $this->getElasticSearch();
    }

    /**
     * @param Query  $query
     * @param string $boolean
     *
     * @return Query
     */
    protected function addNestedWhereQuery(Query $query, string $boolean = 'and'): self
    {
        if (count($query->wheres)) {
            $type = 'Nested';
            $this->wheres[] = compact('type', 'query', 'boolean');
        }

        return $this;
    }
}
