<?php

declare(strict_types=1);

namespace CrCms\ElasticSearch;

use BadMethodCallException;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Ramsey\Uuid\Uuid;
use RuntimeException;

/**
 * @method Builder index(string|array $index)
 * @method Builder type(string $type)
 * @method Builder limit(int $value)
 * @method Builder take(int $value)
 * @method Builder offset(int $value)
 * @method Builder skip(int $value)
 * @method Builder orderBy(string $field, $sort)
 * @method Builder aggBy(string | array $field, $type = null)
 * @method Builder scroll(string $scroll)
 * @method Builder select(string |array $columns)
 * @method Builder whereMatch($field, $value, $boolean = 'and')
 * @method Builder orWhereMatch($field, $value, $boolean = 'or')
 * @method Builder whereTerm($field, $value, $boolean = 'and')
 * @method Builder whereIn($field, array $value):
 * @method Builder orWhereIn($field, array $value): self
 * @method Builder orWhereTerm($field, $value, $boolean = 'or')
 * @method Builder whereRange($field, $operator = null, $value = null, $boolean = 'and')
 * @method Builder orWhereRange($field, $operator = null, $value = null)
 * @method Builder whereBetween($field, array $values, $boolean = 'and')
 * @method Builder whereNotBetween($field, array $values)
 * @method Builder orWhereBetween($field, array $values)
 * @method Builder orWhereNotBetween(string $field, array $values)
 * @method Builder whereExists($field, $boolean = 'and')
 * @method Builder whereNotExists($field, $boolean = 'and')
 * @method Builder where($column, $operator = null, $value = null, string $leaf = 'term', string $boolean = 'and')
 * @method Builder orWhere($field, $operator = null, $value = null, $leaf = 'term')
 * @method Builder whereNested(Closure $callback, string $boolean)
 * @method Builder newQuery()
 * @method Builder getElasticSearch()
 */
class Builder
{
    /**
     * @var Query
     */
    protected $query;

    /**
     * @var array
     */
    protected $queryLogs = [];

    /**
     * @var bool
     */
    protected $enableQueryLog = false;

    /**
     * @param Query $query
     */
    public function __construct(Query $query)
    {
        $this->query = $query;
    }

    /**
     * @return void
     */
    public function resetQuery(): void
    {
        $this->query = $this->query->newQuery();
    }

    /**
     * @return object|null
     */
    public function first(): ?object
    {
        $this->query->limit(1);

        return $this->get()->first();
    }

    /**
     * @return Collection
     */
    public function get(): Collection
    {
        return $this->metaData($this->getOriginal());
    }

    /**
     * @return array
     */
    public function getOriginal(): array
    {
        return $this->runQuery($this->query->getGrammar()->compileSelect($this->query), 'search');
    }

    /**
     * @param int $page
     * @param int $perPage
     *
     * @return Collection
     */
    public function paginate(int $page, int $perPage = 15): Collection
    {
        $from = (($page * $perPage) - $perPage);

        if (empty($this->query->offset)) {
            $this->query->offset($from);
        }

        if (empty($this->query->limit)) {
            $this->query->limit($perPage);
        }

        $results = $this->runQuery($this->query->getGrammar()->compileSelect($this->query));

        $data = $this->metaData($results);

        $maxPage = intval(ceil($results['hits']['total']['value'] / $perPage));

        return Collection::make([
            'total'        => $results['hits']['total']['value'],
            'per_page'     => $perPage,
            'current_page' => $page,
            'next_page'    => $page < $maxPage ? $page + 1 : $maxPage,
            //'last_page' => $maxPage,
            'total_pages' => $maxPage,
            'from'        => $from,
            'to'          => $from + $perPage,
            'data'        => $data,
        ]);
    }

    /**
     * @param string|int $id
     *
     * @return null|object
     */
    public function byId($id): ?object
    {
        $result = $this->query->runQuery(
            $this->query->whereTerm('_id', $id)->getGrammar()->compileSelect($this->query)
        );

        return isset($result['hits']['hits'][0]) ?
            $this->sourceToObject($result['hits']['hits'][0]) :
            null;
    }

    /**
     * @param string|int $id
     *
     * @return object
     */
    public function byIdOrFail($id): object
    {
        $result = $this->byId($id);

        if (empty($result)) {
            throw new RuntimeException('Resource not found by id:'.$id);
        }

        return $result;
    }

    /**
     * @param callable $callback
     * @param int      $limit
     * @param string   $scroll
     *
     * @return bool
     */
    public function chunk(callable $callback, $limit = 2000, $scroll = '10m')
    {
        if (empty($this->query->scroll)) {
            $this->query->scroll($scroll);
        } else {
            $scroll = $this->query->scroll;
        }

        if (empty($this->query->limit)) {
            $this->query->limit($limit);
        } else {
            $limit = $this->query->limit;
        }

        $condition = $this->query->getGrammar()->compileSelect($this->query);
        $results = $this->runQuery($condition, 'search');

        if ($results['hits']['total']['value'] === 0) {
            return;
        }

        // First total eq limit
        $total = $limit;

        $whileNum = intval(floor($results['hits']['total']['value'] / $total));

        do {
            if (call_user_func($callback, $this->metaData($results)) === false) {
                return false;
            }

            $results = $this->runQuery(['scroll_id' => $results['_scroll_id'], 'scroll' => $scroll], 'scroll');

            $total += count($results['hits']['hits']);
        } while ($whileNum--);
    }

    /**
     * @param array           $data
     * @param string|int|null $id
     * @param string          $key
     *
     * @return object
     */
    public function create(array $data, $id = null, $key = 'id'): object
    {
        $id = $id ? $id : (isset($data[$key]) ? $data[$key] : Uuid::uuid6()->toString());

        $result = $this->runQuery(
            $this->query->getGrammar()->compileCreate($this->query, $id, $data),
            'create'
        );

        if (!isset($result['result']) || $result['result'] !== 'created') {
            throw new RunTimeException('Create error, params: '.json_encode($this->query->getLastQueryLog()));
        }

        $data['_id'] = $id;
        $data['_result'] = $result;

        return (object) $data;
    }

    /**
     * @param array           $data
     * @param string|int|null $id
     * @param string          $key
     *
     * @return Collection
     */
    public function createCollection(array $data, $id = null, $key = 'id'): Collection
    {
        return Collection::make($this->create($data, $id, $key));
    }

    /**
     * @param string|int $id
     * @param array      $data
     *
     * @return bool
     */
    public function update($id, array $data): bool
    {
        $result = $this->runQuery($this->query->getGrammar()->compileUpdate($this->query, $id, $data), 'update');

        if (!isset($result['result']) || $result['result'] !== 'updated') {
            throw new RunTimeException('Update error params: '.json_encode($this->query->getLastQueryLog()));
        }

        return true;
    }

    /**
     * @param string|int $id
     *
     * @return bool
     */
    public function delete($id): bool
    {
        $result = $this->runQuery($this->query->getGrammar()->compileDelete($this->query, $id), 'delete');

        if (!isset($result['result']) || $result['result'] !== 'deleted') {
            throw new RunTimeException('Delete error params:'.json_encode($this->query->getLastQueryLog()));
        }

        return true;
    }

    /**
     * @return int
     */
    public function count(): int
    {
        $result = $this->runQuery($this->query->getGrammar()->compileSelect($this->query), 'count');

        return $result['count'];
    }

    /**
     * @param array  $params
     * @param string $method
     *
     * @return mixed
     */
    public function runQuery(array $params, string $method = 'search')
    {
        if ($this->enableQueryLog) {
            $this->queryLogs[] = $params;
        }

        return tap(call_user_func([$this->query->getElasticSearch(), $method], $params), function () {
            $this->resetQuery();
        });
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
        return Arr::last($this->queryLogs);
    }

    /**
     * @param array $results
     *
     * @return Collection
     */
    protected function metaData(array $results): Collection
    {
        return Collection::make($results['hits']['hits'])->map(function ($hit) {
            return $this->sourceToObject($hit);
        });
    }

    /**
     * @param array $result
     *
     * @return object
     */
    protected function sourceToObject(array $result): object
    {
        return (object) array_merge($result['_source'], ['_id' => $result['_id'], '_score' => $result['_score']]);
    }

    /**
     * @param string $name
     * @param array  $arguments
     *
     * @return mixed
     */
    public function __call(string $name, array $arguments)
    {
        if (method_exists($this->query, $name)) {
            $query = call_user_func_array([$this->query, $name], $arguments);
            // If the query instance is returned, it is managed
            if ($query instanceof $this->query) {
                return $this;
            }

            return $query;
        }

        throw new BadMethodCallException(sprintf('The method[%s] not found', $name));
    }
}
