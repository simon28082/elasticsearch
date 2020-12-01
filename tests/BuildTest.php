<?php

declare(strict_types=1);

namespace CrCms\ElasticSearch\Test;

use CrCms\ElasticSearch\Builder;
use CrCms\ElasticSearch\Factory;
use PHPUnit\Framework\TestCase;

class BuildTest extends TestCase
{
    /**
     * @var Builder
     */
    public static $build;

    /**
     *
     * @return void
     */
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        $config = require __DIR__.'/../config/search.php';
        $config['hosts'] = [
            '172.16.12.69:9200'
        ];
        $config['open_log'] = true;

        static::$build = Factory::builder($config);
    }

    /**
     *
     * @return object
     */
    public function testCreate()
    {
        $result = static::$build->index('index')->type('type')->create([
            'key' => 'value'
        ]);

        self::assertObjectHasAttribute('_id', $result);

        return $result;
    }

    /**
     * @depends testCreate
     *
     * @param object $result
     *
     * @return object
     */
    public function testUpdate(object $result)
    {
        $updateResult = static::$build->update($result->_id, ['key' => 'new value']);
        $this->assertTrue($updateResult);
        return $result;
    }

    /**
     * @depends testUpdate
     * @param object $result
     *
     * @return void
     */
    public function testDelete(object $result)
    {
        $deleteResult = static::$build->delete($result->_id);
        $this->assertTrue($deleteResult);
    }

    public function testGet()
    {
        $create = static::$build->index('index1')->create([
            'key' => 'value'
        ]);

        // count
        $count = static::$build->index('index1')->where('key', 'value')->count();
        $this->assertTrue($count > 0);

        // get
        $result = static::$build->index('index1')->where('key', 'value')->get();
        $this->assertTrue($result->count() > 0);
        $this->assertObjectHasAttribute('_id', $result->first());
        $this->assertObjectHasAttribute('_score', $result->first());
        $this->assertObjectHasAttribute('key', $result->first());

        $one = static::$build->index('index1')->where('key', 'vu')->first();
        $this->assertTrue(is_null($one));

        static::$build->enableQueryLog();
        $oneExists = static::$build->index('index1')->where('key', 'value')->first();
        $this->assertTrue(! is_null($oneExists));
        $this->assertObjectHasAttribute('_id', $oneExists);
        $this->assertObjectHasAttribute('_score', $oneExists);
        $this->assertObjectHasAttribute('key', $oneExists);
    }

    public function testChunk()
    {
        $index = uniqid();
        $type = uniqid();
        for ($i = 0; $i <= 10; $i++) {
            $result = static::$build->index($index)->type($type)->create(['value' => $i]);
            $this->assertTrue(! is_null($result));
        }
        static::$build->enableQueryLog();
        $v = static::$build->index($index)->type($type)->count();
        dd($v, 111, static::$build->getLastQueryLog());

        static::$build->index($index)->whereIn('value', [1, 3, 5, 7, 9])->chunk(function ($result) {
            var_dump($result);
            var_dump('@@@@@@@');
        }, 2);
    }
}