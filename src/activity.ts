import { nonempty } from "./codec.js";
export type Parser<T> = (value: unknown) => T;
export interface ActivityDefinition<I, O> {
  readonly name: string;
  readonly input?: Parser<I>;
  readonly output?: Parser<O>;
  /** Type-only invariant markers: definitions with incompatible contracts cannot be substituted. */
  readonly _input?: (value: I) => I;
  readonly _output?: (value: O) => O;
}
export function activity<I, O>(
  name: string,
  parsers: { input?: Parser<I>; output?: Parser<O> } = {},
): ActivityDefinition<I, O> {
  nonempty(name, "Activity name");
  return Object.freeze({ name, ...parsers });
}
