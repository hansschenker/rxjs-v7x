/** @prettier */
import { expect } from 'chai';
import { Observable, Subscriber, Subscription, operate, of, throwError, OperatorFunction } from 'rxjs';

function recorder() {
  const seen: any[] = [];
  const destination = new Subscriber<any>({
    next: (value: any) => seen.push(['N', value]),
    error: (err: any) => seen.push(['E', err]),
    complete: () => seen.push(['C']),
  });
  return { seen, destination };
}

describe('Subscriber overrides', () => {
  it('should forward through the next override', () => {
    const { seen, destination } = recorder();
    const subscriber = new Subscriber<number>(destination, { next: (value) => destination.next(value * 2) });
    subscriber.next(1);
    subscriber.next(2);
    subscriber.complete();
    expect(seen).to.deep.equal([['N', 2], ['N', 4], ['C']]);
  });

  it('should route an error thrown in the next override to the destination', () => {
    const { seen, destination } = recorder();
    const error = new Error('boom');
    const subscriber = new Subscriber<number>(destination, {
      next: (value) => {
        if (value === 2) {
          throw error;
        }
        destination.next(value);
      },
    });
    subscriber.next(1);
    subscriber.next(2);
    subscriber.next(3);
    expect(seen).to.deep.equal([
      ['N', 1],
      ['E', error],
    ]);
    expect(subscriber.closed).to.be.true;
  });

  it('should call the error override instead of forwarding, then unsubscribe', () => {
    const { seen, destination } = recorder();
    const handled: any[] = [];
    const subscriber = new Subscriber<number>(destination, { error: (err) => handled.push(err) });
    subscriber.error('bad');
    expect(handled).to.deep.equal(['bad']);
    expect(seen).to.deep.equal([]);
    expect(subscriber.closed).to.be.true;
  });

  it('should route an error thrown in the error override to the destination', () => {
    const { seen, destination } = recorder();
    const error = new Error('in handler');
    const subscriber = new Subscriber<number>(destination, {
      error: () => {
        throw error;
      },
    });
    subscriber.error('bad');
    expect(seen).to.deep.equal([['E', error]]);
    expect(subscriber.closed).to.be.true;
  });

  it('should call the complete override instead of forwarding, then unsubscribe', () => {
    const { seen, destination } = recorder();
    let completed = 0;
    const subscriber = new Subscriber<number>(destination, { complete: () => completed++ });
    subscriber.complete();
    expect(completed).to.equal(1);
    expect(seen).to.deep.equal([]);
    expect(subscriber.closed).to.be.true;
  });

  it('should route an error thrown in the complete override to the destination', () => {
    const { seen, destination } = recorder();
    const error = new Error('in complete');
    const subscriber = new Subscriber<number>(destination, {
      complete: () => {
        throw error;
      },
    });
    subscriber.complete();
    expect(seen).to.deep.equal([['E', error]]);
    expect(subscriber.closed).to.be.true;
  });

  it('should call finalize exactly once on unsubscribe', () => {
    const { destination } = recorder();
    let finalized = 0;
    const subscriber = new Subscriber<number>(destination, { finalize: () => finalized++ });
    subscriber.unsubscribe();
    subscriber.unsubscribe();
    expect(finalized).to.equal(1);
  });

  it('should call finalize once after complete and once after error', () => {
    let finalized = 0;
    const a = new Subscriber<number>(recorder().destination, { finalize: () => finalized++ });
    a.complete();
    expect(finalized).to.equal(1);
    const b = new Subscriber<number>(recorder().destination, { finalize: () => finalized++ });
    b.error('x');
    expect(finalized).to.equal(2);
  });

  it('should be closed when the destination is unsubscribed', () => {
    const { destination } = recorder();
    let finalized = 0;
    const subscriber = new Subscriber<number>(destination, { finalize: () => finalized++ });
    expect(subscriber.closed).to.be.false;
    destination.unsubscribe();
    expect(subscriber.closed).to.be.true;
    expect(finalized).to.equal(1);
  });

  it('should pass everything through when no overrides are given', () => {
    const { seen, destination } = recorder();
    const subscriber = new Subscriber<number>(destination, {});
    subscriber.next(1);
    subscriber.complete();
    expect(seen).to.deep.equal([['N', 1], ['C']]);
  });
});

describe('operate', () => {
  it('should create a Subscriber wired to the destination', () => {
    const { seen, destination } = recorder();
    const subscriber = operate<number, number>({ destination });
    expect(subscriber).to.be.instanceOf(Subscriber);
    expect(subscriber).to.not.equal(destination);
    subscriber.next(1);
    subscriber.complete();
    expect(seen).to.deep.equal([['N', 1], ['C']]);
  });

  it('should install the overrides from the config', () => {
    const { seen, destination } = recorder();
    let finalized = 0;
    const subscriber = operate<number, number>({
      destination,
      next: (value) => destination.next(value + 100),
      complete: () => destination.next(-1),
      finalize: () => finalized++,
    });
    subscriber.next(1);
    subscriber.complete();
    expect(seen).to.deep.equal([
      ['N', 101],
      ['N', -1],
    ]);
    expect(finalized).to.equal(1);
  });

  function double(): OperatorFunction<number, number> {
    return (source) =>
      new Observable((destination) => {
        source.subscribe(
          operate({
            destination,
            next: (value) => destination.next(value * 2),
          })
        );
      });
  }

  it('should support operators written as new Observable + operate', () => {
    const results: any[] = [];
    of(1, 2, 3)
      .pipe(double())
      .subscribe({
        next: (value) => results.push(value),
        complete: () => results.push('done'),
      });
    expect(results).to.deep.equal([2, 4, 6, 'done']);
  });

  it('should pass source errors through operators written with it', () => {
    const results: any[] = [];
    throwError(() => 'bad')
      .pipe(double())
      .subscribe({
        next: (value) => results.push(value),
        error: (err) => results.push('error: ' + err),
      });
    expect(results).to.deep.equal(['error: bad']);
  });

  it('should be lazy and run the subscribe function once per subscription', () => {
    let runs = 0;
    const source = new Observable<number>((subscriber) => {
      runs++;
      subscriber.next(runs);
      subscriber.complete();
    });
    const doubled = source.pipe(double());
    expect(runs).to.equal(0);
    const a: number[] = [];
    const b: number[] = [];
    doubled.subscribe((value) => a.push(value));
    doubled.subscribe((value) => b.push(value));
    expect(runs).to.equal(2);
    expect(a).to.deep.equal([2]);
    expect(b).to.deep.equal([4]);
  });

  it('should tear down the source when the consumer unsubscribes', () => {
    let teardowns = 0;
    const source = new Observable<number>(() => () => teardowns++);
    const subscription: Subscription = source.pipe(double()).subscribe();
    expect(teardowns).to.equal(0);
    subscription.unsubscribe();
    expect(teardowns).to.equal(1);
  });

  it('should turn a synchronous throw inside the operator into an error notification', () => {
    const error = new Error('setup failed');
    const failing: OperatorFunction<number, number> = () =>
      new Observable(() => {
        throw error;
      });
    const results: any[] = [];
    of(1)
      .pipe(failing)
      .subscribe({ next: (value) => results.push(value), error: (err) => results.push(err) });
    expect(results).to.deep.equal([error]);
  });

  it('should let operators written with it accept any subscribable, with no lift required', () => {
    const subscribable = {
      subscribe(observer: { next: (value: number) => void; complete: () => void }) {
        observer.next(21);
        observer.complete();
        return { unsubscribe() {} };
      },
    };
    const results: any[] = [];
    double()(subscribable as any).subscribe({
      next: (value) => results.push(value),
      complete: () => results.push('done'),
    });
    expect(results).to.deep.equal([42, 'done']);
  });
});
