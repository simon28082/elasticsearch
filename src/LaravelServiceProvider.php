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
        //merge config
        $configFile = $this->packagePath . 'config/search.php';
        if (file_exists($configFile)) {
            $this->mergeConfigFrom($configFile, 'search');
        }

        $this->app->bind(Builder::class, function ($app) {
            return new Builder(
                $this->app->make('config')->get('search'),
                new Grammar(),
                ClientBuilder::create()
                    ->setHosts($this->app->make('config')->get('search.hosts'))
                    ->build());
        });
    }
}