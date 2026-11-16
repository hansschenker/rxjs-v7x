import { Subscriber, SubscriberOverrides } from './Subscriber';

/**
 * Configuration for {@link operate}. `destination` is the downstream {@link Subscriber} that
 * every notification is ultimately forwarded to. The remaining fields are the
 * {@link SubscriberOverrides} installed on the {@link Subscriber} that `operate` creates.
 */
export interface OperateConfig<In, Out> extends SubscriberOverrides<In> {
  /**
   * The downstream subscriber. The created subscriber is added to it, so that tearing
   * down the destination tears down the created subscriber as well.
   */
  destination: Subscriber<Out>;
}

/**
 * Creates a {@link Subscriber} that is wired to `destination` and whose `next`, `error`,
 * `complete` and `finalize` behaviour is replaced by the given overrides. An error thrown
 * inside any override is routed to `destination.error`.
 *
 * This is the building block for writing operators without `lift`:
 *
 * ```ts
 * export function map<T, R>(project: (value: T, index: number) => R): OperatorFunction<T, R> {
 *   return (source) =>
 *     new Observable((destination) => {
 *       let index = 0;
 *       source.subscribe(
 *         operate({
 *           destination,
 *           next: (value: T) => destination.next(project(value, index++)),
 *         })
 *       );
 *     });
 * }
 * ```
 *
 * @param config The destination and the overrides.
 * @return A subscriber to hand to `source.subscribe(...)`.
 */
export function operate<In, Out>({ destination, ...subscriberOverrides }: OperateConfig<In, Out>): Subscriber<In> {
  return new Subscriber(destination, subscriberOverrides);
}
