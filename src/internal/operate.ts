import { Observable } from './Observable';
import { Subscriber, SubscriberOverrides } from './Subscriber';
import { OperatorFunction, TeardownLogic } from './types';

/*
 * An operator has two ends, and each end has its own helper:
 *
 *   source ──> [ fromSource: the operator ] ──> [ toDestination: middle subscriber ] ──> destination
 *
 * fromSource    the source end. Runs at pipe time, once. Returns a new Observable.
 *               RxJS 7.8.2: operate(init), which used lift.
 *               RxJS 8:     no helper, written by hand as (source) => new Observable(...).
 *
 * toDestination the destination end. Runs at subscribe time, once per subscription.
 *               Returns the subscriber that feeds the destination.
 *               RxJS 7.8.2: createOperatorSubscriber(destination, onNext, onComplete, onError, onFinalize).
 *               RxJS 8:     operate({ destination, next, complete, error, finalize }).
 *
 * RxJS 8 kept only the destination helper and gave it the name `operate`. Here both ends are
 * named, and `operate` stays available as an alias of `toDestination`, so operators written
 * for RxJS 8 compile unchanged.
 */

/**
 * Configuration for {@link toDestination}. `destination` is the downstream {@link Subscriber} that
 * every notification is ultimately forwarded to. The remaining fields are the
 * {@link SubscriberOverrides} installed on the {@link Subscriber} that `toDestination` creates.
 */
export interface ToDestinationConfig<In, Out> extends SubscriberOverrides<In> {
  /**
   * The downstream subscriber. The created subscriber is added to it, so that tearing
   * down the destination tears down the created subscriber as well.
   */
  destination: Subscriber<Out>;
}

/**
 * The RxJS 8 name for {@link ToDestinationConfig}.
 */
export type OperateConfig<In, Out> = ToDestinationConfig<In, Out>;

/**
 * The source end of an operator. Turns a function that connects a source to a destination
 * into an {@link OperatorFunction}.
 *
 * `fromSource` runs at pipe time: it returns the function that `pipe` calls with the source,
 * and that function returns a new {@link Observable}. Nothing is subscribed yet. Each time the
 * returned Observable is subscribed, `connect` runs once with the source and that
 * subscription's destination, so state declared inside `connect` is private to one
 * subscription. A function returned from `connect` is called on teardown, and an error thrown
 * synchronously inside `connect` is sent to `destination.error`.
 *
 * It is the named form of `(source) => new Observable((destination) => ...)`, which is how
 * RxJS 8 writes every operator. It takes the place of the RxJS 7.8.2 `operate(init)`, without
 * `lift`.
 *
 * Edge case: an operator whose state is shared by all its subscribers cannot use `fromSource`,
 * because `connect` runs once per subscription. {@link share} keeps its connection, subject and
 * reference count between `(source) =>` and `new Observable(...)`, so they are created once at
 * pipe time, and writes that shape by hand.
 *
 * ```ts
 * export function filter<T>(predicate: (value: T, index: number) => boolean): MonoTypeOperatorFunction<T> {
 *   return fromSource((source, destination) => {
 *     let index = 0;
 *     source.subscribe(
 *       toDestination({
 *         destination,
 *         next: (value) => predicate(value, index++) && destination.next(value),
 *       })
 *     );
 *   });
 * }
 * ```
 *
 * @param connect Called once per subscription with the source and the destination.
 * @return An operator function for `pipe`.
 */
export function fromSource<In, Out>(
  connect: (source: Observable<In>, destination: Subscriber<Out>) => TeardownLogic
): OperatorFunction<In, Out> {
  return (source) => new Observable<Out>((destination) => connect(source, destination));
}

/**
 * The destination end of an operator. Creates the subscriber that sits between the source
 * and `destination`.
 *
 * The created {@link Subscriber} is wired to `destination`: unsubscribing the destination
 * unsubscribes it too. Its `next`, `error`, `complete` and `finalize` behaviour is replaced by
 * the given overrides. A notification without an override passes through to `destination`
 * unchanged, and an error thrown inside any override is sent to `destination.error`.
 *
 * This is the RxJS 8 `operate({ destination, ... })` under the name of the end it works on.
 * It takes the place of the RxJS 7.8.2 `createOperatorSubscriber`.
 *
 * ```ts
 * export function map<T, R>(project: (value: T, index: number) => R): OperatorFunction<T, R> {
 *   return fromSource((source, destination) => {
 *     let index = 0;
 *     source.subscribe(
 *       toDestination({
 *         destination,
 *         next: (value: T) => destination.next(project(value, index++)),
 *       })
 *     );
 *   });
 * }
 * ```
 *
 * @param config The destination and the overrides.
 * @return A subscriber to hand to `source.subscribe(...)`.
 */
export function toDestination<In, Out>({ destination, ...subscriberOverrides }: ToDestinationConfig<In, Out>): Subscriber<In> {
  return new Subscriber(destination, subscriberOverrides);
}

/**
 * The RxJS 8 name for {@link toDestination}. It is the same function, kept so that operators
 * written for RxJS 8 work unchanged.
 */
export const operate: <In, Out>(config: ToDestinationConfig<In, Out>) => Subscriber<In> = toDestination;
