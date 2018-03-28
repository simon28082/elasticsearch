<?php

namespace CrCms\ElasticSearch;

/**
 * Class Grammar
 *
 * @package CrCms\ElasticSearch
 * @author simon
 */
class Grammar
{
    /**
     * @var array
     */
    protected $selectComponents = [
        '_source' => 'columns',
        'query' => 'wheres',
        'aggs',
        'sort' => 'orders',
        'size' => 'limit',
        'from' => 'offset',
        'index' => 'index',
        'type' => 'type',
        'scroll' => 'scroll',
    ];

    /**
     * @param Builder $builder
     * @return int
     */
    public function compileOffset(Builder $builder): int
    {
        return $builder->offset;
    }

    /**
     * @param Builder $builder
     * @return int
     */
    public function compileLimit(Builder $builder): int
    {
        return $builder->limit;
    }

    /**
     * @param Builder $builder
     * @return string
     */
    public function compileScroll(Builder $builder): string
    {
        return $builder->scroll;
    }

    /**
     * @param Builder $builder
     * @return array
     */
    public function compileSelect(Builder $builder)
    {
        $body = $this->compileComponents($builder);
        $index = array_pull($body, 'index');
        $type = array_pull($body, 'type');
        $scroll = array_pull($body, 'scroll');
        $params = ['body' => $body, 'index' => $index, 'type' => $type];
        if ($scroll) {
            $params['scroll'] = $scroll;
        }
        return $params;
    }

    /**
     * @param Builder $builder
     * @param $id
     * @param array $data
     * @return array
     */
    public function compileCreate(Builder $builder, $id, array $data): array
    {
        return array_merge([
            'id' => $id,
            'body' => $data
        ], $this->compileComponents($builder));
    }

    /**
     * @param Builder $builder
     * @param $id
     * @return array
     */
    public function compileDelete(Builder $builder, $id): array
    {
        return array_merge([
            'id' => $id,
        ], $this->compileComponents($builder));
    }

    /**
     * @param Builder $builder
     * @param $id
     * @param array $data
     * @return array
     */
    public function compileUpdate(Builder $builder, $id, array $data): array
    {
        return array_merge([
            'id' => $id,
            'body' => ['doc' => $data]
        ], $this->compileComponents($builder));
    }

    /**
     * @param Builder $builder
     * @return array
     */
    public function compileAggs(Builder $builder): array
    {
        $aggs = [];

        foreach ($builder->aggs as $field => $aggItem) {
            if (is_array($aggItem)) {
                $aggs[] = $aggItem;
            } else {
                $aggs[$field . '_' . $aggItem] = [$aggItem => ['field' => $field]];
            }
        }

        return $aggs;
    }

    /**
     * @param Builder $builder
     * @return array
     */
    public function compileColumns(Builder $builder): array
    {
        return $builder->columns;
    }

    /**
     * @param Builder $builder
     * @return string
     */
    public function compileIndex(Builder $builder): string
    {
        return is_array($builder->index) ? implode(',', $builder->index) : $builder->index;
    }

    /**
     * @param Builder $builder
     * @return string
     */
    public function compileType(Builder $builder): string
    {
        return $builder->type;
    }

    /**
     * @param Builder $builder
     * @return array
     */
    public function compileOrders(Builder $builder): array
    {
        $orders = [];

        foreach ($builder->orders as $field => $orderItem) {
            $orders[$field] = is_array($orderItem) ? $orderItem : ['order' => $orderItem];
        }

        return $orders;
    }

    /**
     * @param Builder $builder
     * @return array
     */
    protected function compileWheres(Builder $builder): array
    {
        $whereGroups = $this->wherePriorityGroup($builder->wheres);

        $operation = count($whereGroups) === 1 ? 'must' : 'should';

        $bool = [];

        foreach ($whereGroups as $wheres) {
            $must = [];

            foreach ($wheres as $where) {
                if ($where['type'] === 'Nested') {
                    $must[] = $this->compileWheres($where['query']);
                } else {
                    //$must[] = [$where['leaf'] => [$where['column'] => $where['value']]];
                    $must[] = $this->whereLeaf($where['leaf'], $where['column'], $where['operator'], $where['value']);
                }
            }

            if (!empty($must)) {
                $bool['bool'][$operation][] = count($must) === 1 ? array_shift($must) : ['bool' => ['must' => $must]];
            }
        }

        return $bool;
    }

    /**
     * @param string $leaf
     * @param string $column
     * @param string|null $operator
     * @param $value
     * @return array
     */
    protected function whereLeaf(string $leaf, string $column, string $operator = null, $value): array
    {
        if (in_array($leaf, ['term', 'match'], true)) {
            return [$leaf => [$column => $value]];
        } elseif ($leaf === 'range') {
            return [$leaf => [
                $column => is_array($value) ? $value : [$operator => $value]
            ]];
        }
    }

    /**
     * @param array $wheres
     * @return array
     */
    protected function wherePriorityGroup(array $wheres): array
    {
        //get "or" index from array
        $orIndex = (array)array_keys(array_map(function ($where) {
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
     * @param Builder $query
     * @return array
     */
    protected function compileComponents(Builder $query): array
    {
        $body = [];

        foreach ($this->selectComponents as $key => $component) {
            if (!empty($query->$component)) {
                $method = 'compile' . ucfirst($component);

                $body[is_numeric($key) ? $component : $key] = $this->$method($query, $query->$component);
            }
        }

        return $body;
    }
}