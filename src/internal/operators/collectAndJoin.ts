import { Observable } from '../Observable';
import { ObservableInput, OperatorFunction } from '../types';
import { identity } from '../util/identity';
import { mapOneOrManyArgs } from '../util/mapOneOrManyArgs';
import { pipe } from '../util/pipe';
import { mergeMap } from './mergeMap';
import { toArray } from './toArray';

/**
 * Collects all nested Observables of the source, then joins them. Shared by {@link combineLatestAll}
 * and {@link zipAll}:
 *
 * ```ts
 * return collectAndJoin(combineLatest, project); // combineLatestAll
 * return collectAndJoin(zip, project); // zipAll
 * ```
 *
 * It is built from other operators only:
 *
 * 1. `toArray()` waits until the source completes and collects its nested Observables. Nothing is
 *    emitted before that.
 * 2. `mergeMap` joins the collected Observables with `join` (`combineLatest` or `zip`).
 * 3. If `project` is given, each joined array is mapped with it.
 *
 * @param join Joins the collected Observables into one, such as `combineLatest` or `zip`.
 * @param project Optional: maps the joined values, given as separate arguments.
 */
export function collectAndJoin<T, R>(join: (sources: ObservableInput<T>[]) => Observable<T>, project?: (...args: any[]) => R) {
  return pipe(
    // Collect all inner sources into an array, and emit them when the
    // source completes.
    toArray() as OperatorFunction<ObservableInput<T>, ObservableInput<T>[]>,
    // Run the join function on the collected array of inner sources.
    mergeMap((sources) => join(sources)),
    // If a projection function was supplied, apply it to each result.
    project ? mapOneOrManyArgs(project) : (identity as any)
  );
}
