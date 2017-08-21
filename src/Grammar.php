<?php

namespace CrCms\ElasticSearch;

/**
 * Class Grammar
 *
 * @package CrCms\Repository\Drives\ElasticSearch
 * @author simon
 */
class Grammar
{

    protected $selectComponents = [
//        'aggregate',
        '_source'=>'columns',
        'joins',
        'query'=>'wheres',
        'aggs',
//        'havings',
        'sort'=>'orders',
        'size'=>'limit',
        'from'=>'offset',
        'unions',
        'lock',
    ];

    /**
     * Compile the components necessary for a select clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return array
     */
    protected function compileComponents(Builder $query)
    {
        $sql = [];

        foreach ($this->selectComponents as $key => $component) {

            // To compile the query, we'll spin through each component of the query and
            // see if that component exists. If it does we'll just call the compiler
            // function for the component which is responsible for making the SQL.
            if (!empty($query->$component)) {
                $method = 'compile'.ucfirst($component);

                $sql[
                    is_numeric($key) ? $component : $key
//                    is_string($key)$component === 'wheres' ? 'query' : $component
                ] = $this->$method($query, $query->$component);
            }
        }

        return $sql;
    }

    public function compileOffset(Builder $builder): int
    {
        return $builder->offset;
    }

    public function compileLimit(Builder $builder): int
    {
        return $builder->limit;
    }

    public function compileSelect(Builder $builder)
    {
        $sql = $this->compileComponents($builder);

//        $sql = [
//            'query' => $sql['wheres'],
//        ];

        echo json_encode($sql);exit();
    }

    public function compileAggs(Builder $builder): array
    {
        $aggs = [];

        foreach ($builder->aggs as $field => $aggItem) {
            if (is_array($aggItem)) {
                $aggs[] = $aggItem;
            } else {
                $aggs[$field.'_'.$aggItem] = [$aggItem=>['field'=>$field]];
            }
        }

        return $aggs;
    }

    public function compileColumns(Builder $builder): array
    {
        return $builder->columns;
        //return ['_source' => $builder->columns];
    }


    public function compileOrders(Builder $builder): array
    {
        $orders = [];
        foreach ($builder->orders as $field => $orderItem) {
            $orders[] = [$field=>is_array($orderItem) ? $orderItem : ['sort'=>$orderItem]];
        }

        return $orders;
    }

    public function compileWheres(Builder $builder): array
    {
        return $this->resolveWhere($builder->wheres);
        //return ['query'=>$this->resolveWhere($builder->wheres)];
    }

    protected function resolveWhere(array $queryWheres): array
    {
        $whereGroups = $this->wherePriorityGroup($queryWheres);

        $operation = count($whereGroups) === 1 ? 'must' : 'should';

        $bool = [];

        foreach ($whereGroups as $wheres) {
            $must = [];

            foreach ($wheres as $where) {
                if ($where['type'] === 'Nested') {
                    $must[] = $this->compileWheres($where['query']);
                } else {
                    //$must[] = [$where['leaf'] => [$where['column'] => $where['value']]];
                    $must[] = $this->whereLeaf($where['leaf'],$where['column'],$where['operator'],$where['value']);
                }
            }

            $bool['bool'][$operation][] = count($must) === 1 ? array_shift($must) : ['bool' => ['must' => $must]];
        }

        return $bool;
    }

    protected function whereLeaf(string $leaf,string $column,string $operator = null, $value)
    {
        if (in_array($leaf,['term','match'],true)) {
            return [$leaf=>[$column => $value]];
        } elseif ($leaf === 'range') {
            return [$leaf => [
                $column => is_array($value) ? $value : [$operator=>$value]
            ]];
        }
    }

    protected function wherePriorityGroup(array $wheres): array
    {
        //get "or" index from array
        $orIndex = (array)array_keys(array_map(function ($where) {
            return $where['boolean'];
        }, $wheres), 'or');

        $lastIndex = $initIndex = 0;$group = [];
        foreach ($orIndex as $index) {
            $group[] = array_slice($wheres, $initIndex, $index - $initIndex);
            $initIndex = $index;
            $lastIndex = $index;
        }

        $group[] = array_slice($wheres, $lastIndex);

        return $group;
    }

}