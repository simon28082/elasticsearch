<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Elasticsearch Connection Host
    |--------------------------------------------------------------------------
    |
    | Set up one or more host connections
    | Examples:
    | 127.0.0.1:9200
    | 127.0.0.1:9300
    |
    */

    'hosts' => [],

    /*
    |--------------------------------------------------------------------------
    | Elasticsearch Connection Pool
    |--------------------------------------------------------------------------
    |
    | Choose the following
    |
    | Elasticsearch\ConnectionPool\StaticNoPingConnectionPool::class
    | Elasticsearch\ConnectionPool\SimpleConnectionPool::class
    | Elasticsearch\ConnectionPool\SniffingConnectionPool::class
    | Elasticsearch\ConnectionPool\StaticConnectionPool::class
    |
    */

    'connection_pool' => Elasticsearch\ConnectionPool\StaticNoPingConnectionPool::class,

    /*
    |--------------------------------------------------------------------------
    | Elasticsearch Selector
    |--------------------------------------------------------------------------
    |
    | Setting the Connection Selector
    |
    | Elasticsearch\ConnectionPool\Selectors\StickyRoundRobinSelector::class
    | Elasticsearch\ConnectionPool\Selectors\RoundRobinSelector::class
    | Elasticsearch\ConnectionPool\Selectors\RandomSelector::class
    |
    */

    'selector' => Elasticsearch\ConnectionPool\Selectors\StickyRoundRobinSelector::class,

    /*
    |--------------------------------------------------------------------------
    | Open elasticsearch log
    |--------------------------------------------------------------------------
    |
    | Set whether the log to open the record
    |
    */

    'open_log' => false,
];
