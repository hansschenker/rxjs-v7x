import { toObservableFrom } from '../observable/toObservableFrom';
import { observeOn } from '../operators/observeOn';
import { subscribeOn } from '../operators/subscribeOn';
import { InteropObservable, SchedulerLike } from '../types';

export function scheduleObservable<T>(input: InteropObservable<T>, scheduler: SchedulerLike) {
  return toObservableFrom(input).pipe(subscribeOn(scheduler), observeOn(scheduler));
}
