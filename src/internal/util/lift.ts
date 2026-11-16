import { Observable } from '../Observable';
import { isFunction } from './isFunction';

/**
 * Used to determine if an object is an Observable with a lift function.
 */
export function hasLift(source: any): source is { lift: InstanceType<typeof Observable>['lift'] } {
  return isFunction(source?.lift);
}
