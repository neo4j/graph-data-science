# Domain Services

When GDS starts, we initialise global scoped/ long-lived services.
Think metrics, or the graph catalogue.
They live from start to finish of a GDS process.

One thing we want to do is, establish a [Parameter Object](https://wiki.c2.com/?ParameterObject) for these services.
It would serve multiple purposes:
 - We could shorten parameter lists, so that code is easier to grok and easier to change.
 - We would have a grouping and therefore marker to distinguish globally scoped things from e.g. request scoped things.
