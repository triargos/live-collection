import { Schema, SchemaAST } from "effect"

type Members = readonly [Schema.Schema.Any, ...Array<Schema.Schema.Any>]

type TagOf<S extends Schema.Schema.Any> = Schema.Schema.Type<S> extends
  { readonly _tag: infer T extends string } ? T : never

type CasesOf<M extends Members> = { readonly [S in M[number] as TagOf<S>]: S }

type Handlers<M extends Members, Out> = {
  readonly [S in M[number] as TagOf<S>]: (value: Schema.Schema.Type<S>) => Out
}

const tagOf = (schema: Schema.Schema.Any): string => {
  const signature = SchemaAST.getPropertySignatures(schema.ast).find((p) => p.name === "_tag")
  if (signature === undefined || !SchemaAST.isLiteral(signature.type)) {
    throw new Error("taggedUnion members must be Schema.TaggedStruct")
  }
  return String(signature.type.literal)
}

/**
 * Internal — not exported from the package. Effect v3 has no `Schema.TaggedUnion`
 * (confirmed absent, not merely renamed), so tagged unions here are a `Schema.Union` of
 * `Schema.TaggedStruct` arms carrying the `cases` and `match` affordances the v4 public
 * API promises: `SyncEvent.cases.Insert.make(...)`, `ResyncTarget.match(target, {...})`.
 *
 * Members are taken variadically and spread straight into `Schema.Union`, which keeps
 * the tuple type and therefore a precise discriminated `.Type`. Building the member
 * list as an `Array` first would widen it and collapse `_tag` narrowing at every call
 * site, so don't.
 *
 * Throws at construction if a member is not a tagged struct — a wiring mistake that
 * should fail at module load, not at the first decode.
 */
export const taggedUnion = <const M extends Members>(...members: M) => {
  const cases = {} as Record<string, Schema.Schema.Any>
  for (const member of members) cases[tagOf(member)] = member
  return Object.assign(Schema.Union(...members), {
    cases: cases as CasesOf<M>,
    match: <Out>(value: Schema.Schema.Type<M[number]>, handlers: Handlers<M, Out>): Out =>
      (handlers as Record<string, (v: unknown) => Out>)[(value as { readonly _tag: string })._tag]!(value)
  })
}
