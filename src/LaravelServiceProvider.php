<?php

declare(strict_types=1);

namespace CrCms\ElasticSearch;

use Illuminate\Support\ServiceProvider;

class LaravelServiceProvider extends ServiceProvider
{
    /**
     * @var string
     */
    protected $packagePath = __DIR__.DIRECTORY_SEPARATOR.'..'.DIRECTORY_SEPARATOR;

    /**
     * @return void
     */
    public function boot()
    {
        $this->publishes([
            $this->packagePath.'config' => config_path(),
        ]);
    }

    /**
     *
     * @return void
     */
    public function register()
    {
        //merge config
        $this->mergeConfig();

        $this->bindBuilder();
    }

    /**
     *
     * @return void
     */
    protected function bindBuilder(): void
    {
        $this->app->singleton(Builder::class, function ($app) {
            return Factory::builder($app->make('config')->get('search'), $app->make('logger'));
        });
    }

    /**
     * @return void
     */
    protected function mergeConfig(): void
    {
        if ($this->isLumen()) {
            $this->app->configure('search');
        }

        $this->mergeConfigFrom($this->packagePath.'config/search.php', 'search');
    }

    /**
     * isLumen.
     *
     * @return bool
     */
    protected function isLumen(): bool
    {
        return $this->app instanceof \Laravel\Lumen\Application;
    }
}
