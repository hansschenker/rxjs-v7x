# <img src="docs_app/src/assets/images/logos/Rx_Logo_S.png" alt="RxJS Logo" width="86" height="86"> RxJS: Reactive Extensions For JavaScript

![CI](https://github.com/reactivex/rxjs/workflows/CI/badge.svg)
[![npm version](https://badge.fury.io/js/rxjs.svg)](http://badge.fury.io/js/rxjs)
[![Join the chat at https://gitter.im/Reactive-Extensions/RxJS](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/Reactive-Extensions/RxJS?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# rxjs-v7x: RxJS 7.8.2 with the RxJS 8 `operate()`, without `lift`

This repository is [RxJS 7.8.2](https://github.com/ReactiveX/rxjs/tree/7.8.2) with one change
taken from RxJS 8: operators are built with the v8 `operate()` primitive, and the `lift`
mechanism is gone. On top of that, the two ends of an operator have explicit names:
`fromSource` and `toDestination`. Everything else, including `Subject`, `Subscription`,
`SafeSubscriber`, the `config` flags and `toPromise`, is the 7.8.2 implementation.

## The two ends of an operator: `fromSource` / `toDestination`

An operator sits between a source and a destination, and each end has its own helper:

```text
source ──> [ fromSource: the operator ] ──> [ toDestination: middle subscriber ] ──> destination
```

| Helper          | End             | Runs                                     | Returns                              |
| --------------- | --------------- | ---------------------------------------- | ------------------------------------ |
| `fromSource`    | the source      | at pipe time, once                       | a new `Observable`                   |
| `toDestination` | the destination | at subscribe time, once per subscription | the subscriber that feeds the destination |

Every operator in this repository is written with the pair:

```ts
import { OperatorFunction, fromSource, toDestination } from 'rxjs';

export function map<T, R>(project: (value: T, index: number) => R): OperatorFunction<T, R> {
  return fromSource((source, destination) => {
    let index = 0;
    source.subscribe(
      toDestination({
        destination,
        next: (value: T) => destination.next(project(value, index++)),
      })
    );
  });
}
```

`fromSource(connect)` is `(source) => new Observable((destination) => connect(source, destination))`.
`connect` runs once per subscription, so state declared in it is private to that subscription.

`toDestination({ destination, next, error, complete, finalize })` returns a `Subscriber` wired to
`destination`. A notification without an override passes through unchanged, an error thrown in an
override goes to `destination.error`, and unsubscribing the destination unsubscribes it too.

### How the pair maps to RxJS 7.8.2 and RxJS 8

| End             | RxJS 7.8.2                                                            | RxJS 8                                               | rxjs-v7x        |
| --------------- | --------------------------------------------------------------------- | ---------------------------------------------------- | --------------- |
| the source      | `operate(init)`, which calls `source.lift(...)`                       | no helper: `(source) => new Observable(...)` by hand | `fromSource`    |
| the destination | `createOperatorSubscriber(destination, onNext, onComplete, onError, onFinalize)` | `operate({ destination, next, complete, error, finalize })` | `toDestination` |

RxJS 8 kept only the destination helper and gave it the name `operate`, the name 7.8.2 used for the
source helper. Here `operate` stays exported as an alias: `operate === toDestination`, so operators
written for RxJS 8 compile unchanged.

### Edge case: `share`

`share` is the one operator that does not use `fromSource`. Its connection, subject and reference
count must be shared by all subscribers, so it creates them once at pipe time, between
`(source) =>` and `new Observable(...)`. `fromSource` runs its function once per subscription, which
would give every subscriber its own connection.

## How this fork was built

The change is four commits on top of the `7.8.2` tag:

1. `feat(operate)`: `Subscriber` accepts `SubscriberOverrides` (`next`, `error`, `complete`,
   `finalize`) as a second constructor argument, and the public `operate({ destination, ... })`
   returns a `Subscriber` wired to `destination`. Exported from the package root together with
   the `OperateConfig` and `SubscriberOverrides` types.
2. `refactor(operators)`: all 69 operator implementations and the internal helpers follow the
   v8 shape. `OperatorSubscriber` and `createOperatorSubscriber` are removed.
3. `refactor!`: `Observable.lift`, `Observable#source`, `Observable#operator`, `Subject.lift`,
   `WebSocketSubject.lift` and the `Operator` type are removed. `Observable.subscribe` has a
   single path. A `Subject` piped through an operator is a plain `Observable`.
4. `feat(operate)`: `fromSource` and `toDestination` name the two ends of an operator, with the
   `ToDestinationConfig` type. `operate` and `OperateConfig` remain as aliases. Every operator and
   internal helper except `share` is written with the pair, and the creation functions
   (`combineLatest`, `forkJoin`, `zip`, `race`, …) use `toDestination`.

Because no operator calls `lift`, the built-in operators accept any object whose `subscribe`
takes an observer and returns something unsubscribable, with no `from()` required.

Verification on TypeScript 4.2.4 (the toolchain pinned by 7.8.2): `npm run compile`,
`npm run dtslint`, `npm run lint_src`, `npm run lint_spec`, `npm run test:esm`,
`npm run test:circular`, `npm run build:global` and `node tools/prepare-package.js` all pass.
`npm test` with the four real-timer spec files excluded (`buffer`, `delayWhen`, `skipUntil`,
`window`) reports 3164 passing, 4 pending and the two pre-existing environment failures (`ajax`
asserts V8's old `JSON.parse` message, `webSocket` expects no global `WebSocket`). The four
real-timer files abort mocha on Windows timer granularity. On Linux they run, and `skipUntil` and
`window` fail intermittently on the unchanged 7.8.2 code as well. `npm run build:package` and
`npm run lint` only fail on npm 11 because of the `npm-run-all` + `config.commitizen`
incompatibility inherited from 7.8.2; run their steps directly.

Contributors to this fork: Hans Schenker, with Claude (Anthropic).

The original README follows.

---

# The Roadmap from RxJS 7 to 8

Curious what's next for RxJS? Follow along with [Issue 6367](https://github.com/ReactiveX/rxjs/issues/6367).

# RxJS 7

### FOR 6.X PLEASE GO TO [THE 6.x BRANCH](https://github.com/ReactiveX/rxjs/tree/6.x)

Reactive Extensions Library for JavaScript. This is a rewrite of [Reactive-Extensions/RxJS](https://github.com/Reactive-Extensions/RxJS) and is the latest production-ready version of RxJS. This rewrite is meant to have better performance, better modularity, better debuggable call stacks, while staying mostly backwards compatible, with some breaking changes that reduce the API surface.

[Apache 2.0 License](LICENSE.txt)

- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Contribution Guidelines](CONTRIBUTING.md)
- [Maintainer Guidelines](docs_app/content/maintainer-guidelines.md)
- [API Documentation](https://rxjs.dev/)

## Versions In This Repository

- [master](https://github.com/ReactiveX/rxjs/commits/master) - This is all of the current work, which is against v7 of RxJS right now
- [6.x](https://github.com/ReactiveX/rxjs/tree/6.x) - This is the branch for version 6.X

Most PRs should be made to **master**.

## Important

By contributing or commenting on issues in this repository, whether you've read them or not, you're agreeing to the [Contributor Code of Conduct](CODE_OF_CONDUCT.md). Much like traffic laws, ignorance doesn't grant you immunity.

## Installation and Usage

### ES6 via npm

```shell
npm install rxjs
```

It's recommended to pull in the Observable creation methods you need directly from `'rxjs'` as shown below with `range`.
If you're using RxJS version 7.2 or above, you can pull in any operator you need from the same spot, `'rxjs'`.

```ts
import { range, filter, map } from 'rxjs';

range(1, 200)
  .pipe(
    filter(x => x % 2 === 1),
    map(x => x + x)
  )
  .subscribe(x => console.log(x));
```

If you're using RxJS version below 7.2, you can pull in any operator you need from one spot, under `'rxjs/operators'`.

```ts
import { range } from 'rxjs';
import { filter, map } from 'rxjs/operators';

range(1, 200)
  .pipe(
    filter(x => x % 2 === 1),
    map(x => x + x)
  )
  .subscribe(x => console.log(x));
```

### CDN

For CDN, you can use [unpkg](https://unpkg.com/):

[https://unpkg.com/rxjs@^7/dist/bundles/rxjs.umd.min.js](https://unpkg.com/rxjs@%5E7/dist/bundles/rxjs.umd.min.js)

The global namespace for rxjs is `rxjs`:

```js
const { range } = rxjs;
const { filter, map } = rxjs.operators;

range(1, 200)
  .pipe(
    filter(x => x % 2 === 1),
    map(x => x + x)
  )
  .subscribe(x => console.log(x));
```

## Goals

- Smaller overall bundles sizes
- Provide better performance than preceding versions of RxJS
- To model/follow the [Observable Spec Proposal](https://github.com/zenparsing/es-observable) to the observable
- Provide more modular file structure in a variety of formats
- Provide more debuggable call stacks than preceding versions of RxJS

## Building/Testing

- `npm run compile` build everything
- `npm test` run tests
- `npm run dtslint` run dtslint tests

## Adding documentation

We appreciate all contributions to the documentation of any type. All of the information needed to get the docs app up and running locally as well as how to contribute can be found in the [documentation directory](./docs_app).
