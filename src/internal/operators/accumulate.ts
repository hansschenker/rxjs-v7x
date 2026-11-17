import { Observable } from '../Observable';
import { Subscriber } from '../Subscriber';
import { toDestination } from '../operate';

/**
 * When {@link accumulate} emits the accumulated state:
 *
 * - `'each'`: after every source value. Used by {@link scan}.
 * - `'final'`: once, when the source completes. Used by {@link reduce}.
 */
export type AccumulateEmit = 'each' | 'final';

/**
 * Accumulates a state from the source values. Shared by {@link scan} and {@link reduce}.
 *
 * Returns the function to hand to `fromSource`:
 *
 * ```ts
 * // scan
 * return fromSource(accumulate(accumulator, seed, hasSeed, 'each'));
 * // reduce
 * return fromSource(accumulate(accumulator, seed, hasSeed, 'final'));
 * ```
 *
 * `accumulate(...)` itself runs once, when the operator is created, and only captures its
 * arguments. The returned function runs once per subscription, so the state, the index and
 * whether there is a state yet are private to that subscription.
 *
 * @param accumulator Called with the current state, the value and its index; returns the new state.
 * @param seed The initial state.
 * @param hasSeed Whether a seed was given. Without one, the first value becomes the state and the
 * accumulator is not called for it. This is a separate flag because `undefined` is a valid seed.
 * @param emit When to emit the state: after every value, or once at completion.
 */
export function accumulate<V, A, S>(
  accumulator: (acc: V | A | S, value: V, index: number) => A,
  seed: S,
  hasSeed: boolean,
  emit: AccumulateEmit
) {
  return (source: Observable<V>, destination: Subscriber<any>) => {
    // Whether we have a state yet. Without a seed, the first value becomes the state.
    let hasState = hasSeed;
    // The accumulated state. Starts as the seed, if there is one.
    let state: any = seed;
    // The index of the current value, passed to the accumulator.
    let index = 0;

    source.subscribe(
      toDestination({
        destination,
        next: (value) => {
          const i = index++;
          // With a state, the accumulator computes the new one. Without one (no seed),
          // the first value becomes the state as it is.
          state = hasState ? accumulator(state, value, i) : ((hasState = true), value);

          if (emit === 'each') {
            destination.next(state);
          }
        },
        // 'each' passes completion through. 'final' emits the last state first, if there is one.
        complete:
          emit === 'final'
            ? () => {
                if (hasState) {
                  destination.next(state);
                }
                destination.complete();
              }
            : undefined,
      })
    );
  };
}
