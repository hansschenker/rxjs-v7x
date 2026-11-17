import { toObservableFrom } from '../observable/toObservableFrom';
import { observeOn } from '../operators/observeOn';
import { subscribeOn } from '../operators/subscribeOn';
import { SchedulerLike } from '../types';

export function schedulePromise<T>(input: PromiseLike<T>, scheduler: SchedulerLike) {
  return toObservableFrom(input).pipe(subscribeOn(scheduler), observeOn(scheduler));
}
