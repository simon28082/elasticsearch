<?php

declare(strict_types=1);

namespace CrCms\ElasticSearch;

use Illuminate\Support\Arr;

class Grammar
{
    /**
     * @var array
     */
    protected $selectComponents = [
        '_source' => 'columns',
        'query'   => 'wheres',
        'aggs',
        'sort'   => 'orders',
        'size'   => 'limit',
        'from'   => 'offset',
        'index'  => 'index',
        'type'   => 'type',
        'scroll' => 'scroll',
    ];

    /**
     * @param Query $builder
     *
     * @return int
     */
    public function compileOffset(Query $builder): int
    {
        return $builder->offset;
    }

    /**
     * @param Query $builder
     *
     * @return int
     */
    public function compileLimit(Query $builder): int
    {
        return $builder->limit;
    }

    /**
     * @param Query $builder
     *
     * @return string
     */
    public function compileScroll(Query $builder): string
    {
        return $builder->scroll;
    }

    /**
     * @param Query $builder
     *
     * @return array
     */
    public function compileSelect(Query $builder)
    {
        $body = $this->compileComponents($builder);
        $index = Arr::pull($body, 'index');
        $type = Arr::pull($body, 'type');
        $scroll = Arr::pull($body, 'scroll');
        $params = ['body' => $body, 'index' => $index, 'type' => $type];
        if ($scroll) {
            $params['scroll'] = $scroll;
        }

        return $params;
    }

    /**
     * @param Query $builder
     * @param $id
     * @param array $data
     *
     * @return array
     */
    public function compileCreate(Query $builder, $id, array $data): array
    {
        return array_merge([
            'id'   => $id,
            'body' => $data,
        ], $this->compileComponents($builder));
    }

    /**
     * @param Query $builder
     * @param $id
     *
     * @return array
     */
    public function compileDelete(Query $builder, $id): array
    {
        return array_merge([
            'id' => $id,
        ], $this->compileComponents($builder));
    }

    /**
     * @param Query $builder
     * @param $id
     * @param array $data
     *
     * @return array
     */
    public function compileUpdate(Query $builder, $id, array $data): array
    {
        return array_merge([
            'id'   => $id,
            'body' => ['doc' => $data],
        ], $this->compileComponents($builder));
    }

    /**
     * @param Query $builder
     *
     * @return array
     */
    public function compileAggs(Query $builder): array
    {
        $aggs = [];

        foreach ($builder->aggs as $field => $aggItem) {
            if (is_array($aggItem)) {
                $aggs[] = $aggItem;
            } else {
                $aggs[$field.'_'.$aggItem] = [$aggItem => ['field' => $field]];
            }
        }

        return $aggs;
    }

    /**
     * @param Query $builder
     *
     * @return array
     */
    public function compileColumns(Query $builder): array
    {
        return $builder->columns;
    }

    /**
     * @param Query $builder
     *
     * @return string
     */
    public function compileIndex(Query $builder): string
    {
        return is_array($builder->index) ? implode(',', $builder->index) : $builder->index;
    }

    /**
     * @param Query $builder
     *
     * @return string
     */
    public function compileType(Query $builder): string
    {
        return $builder->type;
    }

    /**
     * @param Query $builder
     *
     * @return array
     */
    public function compileOrders(Query $builder): array
    {
        $orders = [];

        foreach ($builder->orders as $field => $orderItem) {
            $orders[$field] = is_array($orderItem) ? $orderItem : ['order' => $orderItem];
        }

        return $orders;
    }

    /**
     * @param Query $builder
     *
     * @return array
     */
    protected function compileWheres(Query $builder): array
    {
        $whereGroups = $this->wherePriorityGroup($builder->wheres);

        $operation = count($whereGroups) === 1 ? 'must' : 'should';

        $bool = [];

        foreach ($whereGroups as $wheres) {
            $must = [];
            $mustNot = [];
            foreach ($wheres as $where) {
                if ($where['type'] === 'Nested') {
                    $must[] = $this->compileWheres($where['query']);
                } else {
                    if ($where['operator'] == 'ne') {
                        $mustNot[] = $this->whereLeaf($where['leaf'], $where['column'], $where['operator'], $where['value']);
                    } else {
                        $must[] = $this->whereLeaf($where['leaf'], $where['column'], $where['operator'], $where['value']);
                    }
                }
            }

            if (!empty($must)) {
                $bool['bool'][$operation][] = count($must) === 1 ? array_shift($must) : ['bool' => ['must' => $must]];
            }
            if (!empty($mustNot)) {
                if ($operation == 'should') {
                    foreach ($mustNot as $not) {
                        $bool['bool'][$operation][] = ['bool'=>['must_not'=>$not]];
                    }
                } else {
                    $bool['bool']['must_not'] = $mustNot;
                }
            }
        }

        return $bool;
    }

    /**
     * @param string      $leaf
     * @param string      $column
     * @param string|null $operator
     * @param $value
     *
     * @return array
     */
    protected function whereLeaf(string $leaf, string $column, string $operator = null, $value): array
    {
        if (strpos($column, '@') !== false) {
            $columnArr = explode('@', $column);
            $ret = ['nested'=>['path'=>$columnArr[0]]];
            $ret['nested']['query']['bool']['must'][] = $this->whereLeaf($leaf, implode('.', $columnArr), $operator, $value);

            return $ret;
        }
        if (in_array($leaf, ['term', 'match', 'terms', 'match_phrase'], true)) {
            return [$leaf => [$column => $value]];
        } elseif ($leaf === 'range') {
            return [$leaf => [
                $column => is_array($value) ? $value : [$operator => $value],
            ]];
        } elseif ($leaf === 'multi_match') {
            return ['multi_match' => [
                'query'  => $value,
                'fields' => (array) $column,
                'type'   => 'phrase',
            ],
            ];
        } elseif ($leaf === 'wildcard') {
            return ['wildcard' => [
                $column => '*'.$value.'*',
            ],
            ];
        } elseif ($leaf === 'exists') {
            return ['exists' => [
                'field' => $column,
            ]];
        }
    }

    /**
     * @param array $wheres
     *
     * @return array
     */
    protected function wherePriorityGroup(array $wheres): array
    {
        //get "or" index from array
        $orIndex = (array) array_keys(array_map(function ($where) {
            return $where['boolean'];
        }, $wheres), 'or');

        $lastIndex = $initIndex = 0;
        $group = [];
        foreach ($orIndex as $index) {
            $group[] = array_slice($wheres, $initIndex, $index - $initIndex);
            $initIndex = $index;
            $lastIndex = $index;
        }

        $group[] = array_slice($wheres, $lastIndex);

        return $group;
    }

    /**
     * @param Query $query
     *
     * @return array
     */
    protected function compileComponents(Query $query): array
    {
        $body = [];

        foreach ($this->selectComponents as $key => $component) {
            if (!empty($query->$component)) {
                $method = 'compile'.ucfirst($component);

                $body[is_numeric($key) ? $component : $key] = $this->$method($query, $query->$component);
            }
        }

        return $body;
    }
}
