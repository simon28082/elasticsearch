<?php

namespace CrCms\ElasticSearch;

use Elasticsearch\ClientBuilder;
use Illuminate\Support\ServiceProvider;

/**
 * Class LaravelServiceProvider
 *
 * @package CrCms\ElasticSearch
 * @author simon
 */
class LaravelServiceProvider extends ServiceProvider
{
    /**
     * @var bool
     */
    protected $defer = true;

    /**
     * @var string
     */
    protected $packagePath = __DIR__ . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR;

    /**
     * @return void
     */
    public function boot()
    {
        $this->publishes([
            $this->packagePath . 'config' => config_path(),
        ]);
    }

    /**
     * @return void
     */
    public function register()
    {
        $this->app->bind(Builder::class, function ($app) {
            return new Builder(
                $this->app->make('config')->get('search'),
                new Grammar(),
                ClientBuilder::create()
                    ->setHosts($this->app->make('config')->get('search.hosts'))
                    ->build());
        });
    }

    /**
     * @return array
     */
    public function provides(): array
    {
        return [
            Builder::class
        ];
    }
}