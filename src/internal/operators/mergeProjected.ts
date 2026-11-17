import { Observable } from '../Observable';
import { toObservableFrom } from '../observable/toObservableFrom';
import { Subscriber } from '../Subscriber';
import { ObservableInput, TeardownLogic } from '../types';
import { toDestination } from '../operate';

/**
 * Options for the two variants built on {@link mergeProjected}. {@link mergeMap} uses none of them.
 */
export interface MergeProjectedOptions<R> {
  /**
   * Called with each value of a nested Observable, before it is forwarded. {@link mergeScan}
   * uses it to store the value as its new state.
   */
  onNestedValue?: (value: R) => void;
  /**
   * Makes the merge recursive, for {@link expand}: each source value is also emitted, and every
   * value of a nested Observable is fed back in as a new source value instead of being forwarded.
   */
  recursive?: boolean;
  /**
   * Extra cleanup when the subscription is torn down. {@link mergeScan} uses it to drop its state.
   */
  finalize?: () => void;
}

/**
 * Projects each source value to a nested Observable and merges their values into one output.
 * Shared by {@link mergeMap} (and through it `concatMap`, `mergeAll` and `concatAll`),
 * {@link mergeScan} and {@link expand}.
 *
 * - At most `concurrent` nested Observables are subscribed at once. A source value that arrives
 *   while all slots are taken waits, and starts when a nested Observable completes, oldest first.
 * - The output completes when the source has completed, no value is waiting, and every nested
 *   Observable has completed. An error from the source or from any nested Observable is
 *   forwarded at once.
 *
 * Returns the function to hand to `fromSource`:
 *
 * ```ts
 * return fromSource(mergeProjected(project, concurrent)); // mergeMap
 * return fromSource(mergeProjected(project, concurrent, { recursive: true })); // expand
 * ```
 *
 * `mergeProjected(...)` itself only captures its arguments. The returned function runs once per
 * subscription, so the waiting values, the count of active nested Observables and the index are
 * private to that subscription.
 *
 * @param project Turns a source value and its index into a nested Observable (or any
 * {@link ObservableInput}).
 * @param concurrent How many nested Observables may be subscribed at the same time.
 * @param options Used by `mergeScan` and `expand` only.
 */
export function mergeProjected<T, R>(
  project: (value: T, index: number) => ObservableInput<R>,
  concurrent: number,
  { onNestedValue, recursive = false, finalize }: MergeProjectedOptions<R> = {}
) {
  return (source: Observable<T>, destination: Subscriber<R>): TeardownLogic => {
    // Source values that arrived while every slot was taken, oldest first.
    const waiting: T[] = [];
    // The number of nested Observables subscribed right now.
    let active = 0;
    // The index passed to `project`.
    let index = 0;
    // Whether the source has completed.
    let sourceComplete = false;

    // The output completes when the source has completed, nothing is waiting,
    // and no nested Observable is still active.
    const completeIfDone = () => {
      if (sourceComplete && !waiting.length && !active) {
        destination.complete();
      }
    };

    // Start a nested Observable if a slot is free, otherwise let the value wait.
    const handleSourceValue = (value: T) => (active < concurrent ? startNested(value) : waiting.push(value));

    const startNested = (value: T) => {
      // A recursive merge (expand) emits every value it starts from, including
      // the values fed back from nested Observables.
      if (recursive) {
        destination.next(value as any);
      }

      active++;

      // Set only when this nested Observable completes normally, not when it
      // errors or is unsubscribed. Checked in `finalize` below.
      let nestedComplete = false;

      toObservableFrom(project(value, index++)).subscribe(
        toDestination({
          destination,
          next: (nestedValue) => {
            onNestedValue?.(nestedValue);

            if (recursive) {
              // Feed the value back in as a new source value. It is emitted when it starts.
              handleSourceValue(nestedValue as any);
            } else {
              destination.next(nestedValue);
            }
          },
          complete: () => {
            // Only make a note. Freeing the slot happens in `finalize`, after this
            // nested subscription has been torn down.
            nestedComplete = true;
          },
          finalize: () => {
            if (nestedComplete) {
              // This runs during teardown, outside any handler that would catch an
              // error, and `startNested` calls the user's `project`. So catch it here
              // and send it to the destination.
              try {
                // The slot is free again.
                active--;
                // Start waiting values, oldest first, while slots are free.
                while (waiting.length && active < concurrent) {
                  startNested(waiting.shift()!);
                }
                completeIfDone();
              } catch (err) {
                destination.error(err);
              }
            }
          },
        })
      );
    };

    source.subscribe(
      toDestination({
        destination,
        next: handleSourceValue,
        complete: () => {
          // The source completing does not complete the output on its own:
          // nested Observables may still be running or waiting.
          sourceComplete = true;
          completeIfDone();
        },
      })
    );

    // Extra cleanup for when the destination is torn down. The subscriptions
    // above are torn down through the destination already.
    return () => {
      finalize?.();
    };
  };
}
