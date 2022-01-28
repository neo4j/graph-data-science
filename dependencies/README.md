# Local-hosted dependencies for GDS

This directory is a host for GDS dependencies, local to the source code.
This reduces the need to depend on internet connections to download necessary binaries.

## Plugins

The `plugins` directory hosts a Maven repository for custom Gradle plugins.

### Publishing a custom plugin 

For plugins we write ourselves, we should use the [Gradle standard way](https://docs.gradle.org/current/userguide/custom_plugins.html) (`buildSrc`).
For plugins that we find online that we may need to fork and update, we can publish special versions of them here.
Using the [Maven Publish](https://docs.gradle.org/current/userguide/publishing_maven.html) plugin the following configuration could work:

```
plugins {
  id 'maven-publish'
}

publishing {
    repositories {
        maven {
            name = 'gdsplugins'
            url = '/path/to/gds/public/dependencies/plugins/repository'
        }
    }
}
```

Remember to modify the path to match your local file system.
Also note that the configuration may need to be inserted into existing `plugins` and `publishing` definitions.
Then, assuming the project uses Gradle Wrapper, you can run:

```
./gradlew publishAllPublicationsToGdspluginsRepository
```

And now they should end up in `plugins/repository`.
Enjoy!
