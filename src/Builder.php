<?php

namespace CrCms\ElasticSearch;

/**
 * Class Builder
 *
 * @package CrCms
 */
class Builder
{
    public $wheres = [];

    public $columns = [];

    public $operators = [
        '=' => 'eq',
        '>' => 'gt',
        '>=' => 'gte',
        '<' => 'lt',
        '<=' => 'lte',
    ];

    public $offset = null;

    public $limit = null;

    public $orders = [];

    public $aggs = [];

    public $index = '';

    public $type = '';

    protected $grammar = null;

    public function __construct()
    {
        $this->grammar =new Grammar();
    }


    public function limit(int $value)
    {
        $this->limit = $value;
        return $this;
    }

    public function offset(int $value)
    {
        $this->offset = $value;
        return $this;
    }

    public function orderBy(string $field,$sort)
    {
        $this->orders[$field] = $sort;
        return $this;
    }

    public function agg($field,$type)
    {
        is_array($field) ?
            $this->aggs[] = $field :
            $this->aggs[$field] = $type;
        return $this;
    }

    public function select( $columns)
    {
        $this->columns = is_array($columns) ? $columns : func_get_args();
        return $this;
    }


    public function whereMatch($field, $value, $boolean = 'and')
    {
        return $this->where($field, '=', $value, 'match', $boolean);
    }

    public function orWhereMatch($field, $value, $boolean = 'and')
    {
        return $this->whereMatch($field, $value, $boolean);
    }


    public function whereTerm($field, $value, $boolean = 'and')
    {
        return $this->where($field, '=', $value, 'term', $boolean);
    }

    public function orWhereTerm($field, $value, $boolean = 'or')
    {
        return $this->whereTerm($field, $value, $boolean);
    }


    public function whereRange($field, $operator = null, $value = null, $boolean = 'and')
    {
        return $this->where($field, $operator, $value, 'range', $boolean);
    }

    public function orWhereRange($field, $operator = null, $value = null)
    {
        return $this->where($field, $operator, $value, 'or');
    }

    public function whereBetween($field, array $values, $boolean = 'and')
    {
        return $this->where($field, null, $values, 'range', $boolean);
    }

    public function orWhereBetween($field, array $values)
    {
        return $this->whereBetween($field, $values, 'or');
    }

    public function where($column, $operator = null, $value = null, $leaf = 'term', $boolean = 'and')
    {

        if ($column instanceof \Closure) {
            return $this->whereNested($column, $boolean);
        }

        if (func_num_args() == 2) {
            list($value, $operator) = [$operator, '='];
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
//        $column = $field;


        $operator = $operator ? $this->operators[$operator] : $operator;

        $this->wheres[] = compact(
            'type', 'column', 'leaf', 'value', 'boolean', 'operator'
        );

        return $this;
    }

    public function orWhere($field, $operator = null, $value = null, $leaf = 'term')
    {
        if (func_num_args() === 2) {
            list($value, $operator) = [$operator, '='];
        }
        return $this->where($field, $operator, $value, $leaf, 'or');
    }

    public function whereNested(\Closure $callback, $boolean)
    {
        $query = $this->newQuery();

        call_user_func($callback, $query);

        return $this->addNestedWhereQuery($query, $boolean);
    }

    public function addNestedWhereQuery($query, $boolean = 'and')
    {
        if (count($query->wheres)) {
            $type = 'Nested';

            $this->wheres[] = compact('type', 'query', 'boolean');
        }

        return $this;
    }

    public function newQuery()
    {
        return new static();
    }





    public function get()
    {
        $this->grammar->compileSelect($this);

        exit();

        $a = $this->resolveWhere($this);
//        $a = $this->resolve($a);
//        dd($a);

        echo json_encode($a);
        exit();
//dd
//;
        dd($this->result);
//$a = $this->resolveBool($a);

        echo json_encode($a);
        exit();
    }


    protected function resolveBool(array $bool, $newBool = [])
    {
        $isOr = collect($bool)->search(function ($wheres) {
            return !collect($wheres)->where('boolean', 'or')->isEmpty();
        });

        foreach ($bool as $wheres) {
            $must = [];

            foreach ($wheres as $where) {
                $must[] = [$where['leaf'] => [$where['column'] => $where['value']]];
            }

            $newBool[]['bool']['must'] = $must;

        }

        if ($isOr === false) {
            return $newBool;
        } else {
            return ['should' => $newBool];
        }

    }


//    protected function resolveBool(array $bool,$newBool = [])
//    {
//        foreach ($bool as $item) {
//
//            if (isset($item['bool'])) {
//                $newBool = $this->resolveBool($item['bool'],$newBool);
//            }
//
//            if (isset($item['should'])) {
//                $newBool['should'] = $item['should'];
//            }
//
//            if (isset($item['must'])) {
//                $newBool['should'] = array_merge([['bool'=>['must'=>$item['must']]]],$newBool['should']);
//                //$newBool['should'] = array_merge($item['must'],$newBool['should']);
//            }
//        }
//
//        return $newBool;
//    }


}