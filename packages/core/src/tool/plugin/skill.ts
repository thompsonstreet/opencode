export * as SkillTool from "./skill.js"

import type { Context } from "@opencode/plugin/effect/plugin"
import { ToolFailure } from "@opencode/ai/effect"
import { Effect, Schema } from "effect"
import { FSUtil } from "@opencode/util/fs-util"
import { Skill } from "../../skill.js"
import { Permission } from "../../permission.js"
import { Mcp } from "../../mcp/index.js"
import { McpSkillPlugin } from "../../plugin/mcp-skill.js"

export const name = "skill"

export const Input = Schema.Struct({
  id: Skill.ID.annotate({ description: "The ID of an available skill or a skill explicitly referenced by the user" }),
})

export const Output = Schema.Struct({
  directory: Schema.String,
  content: Schema.String,
})
export const description = [
  "Load a specialized skill's instructions and resources into the current conversation when the task at hand matches its description.",
  "",
  "The skill ID must match an available skill or a skill explicitly referenced by the user.",
].join("\n")

export const toModelOutput = Skill.toModelOutput

const unableToLoad = (name: string, error?: unknown) =>
  new ToolFailure({ message: `Unable to load skill ${name}`, error })

export const Plugin = {
  id: "opencode.tool.skill",
  effect: Effect.fn("SkillTool.Plugin")(function* (ctx: Context) {
    const fs = yield* FSUtil.Service
    const skills = yield* Skill.Service
    const permission = yield* Permission.Service
    const mcp = yield* Mcp.Service

    // A skill an MCP server names by address is confirmed and registered, then loaded by its ID like any other, so it
    // goes through the same permission check.
    const confirm = Effect.fn("SkillTool.confirm")(function* (target: { server: string; uri: string }) {
      const confirmed = yield* mcp.getSkill(target)
      const id = McpSkillPlugin.ids(yield* mcp.skills()).find(
        (item) => item.skill.server === confirmed.server && item.skill.entry.uri === confirmed.entry.uri,
      )?.id
      if (!id) return yield* unableToLoad(`${target.server}:${target.uri}`)
      return yield* new ToolFailure({
        message: `That address names the skill ${id}. Load it with the skill tool and the ID "${id}".`,
      })
    })
    yield* ctx.tool
      .transform((editor) =>
        editor.add({
          name,
          options: { codemode: false },
          description,
          input: Input,
          output: Output,
          execute: (input, context) =>
            Effect.gen(function* () {
              const skill = yield* skills.get(input.id)
              if (!skill) {
                const target = McpSkillPlugin.address(input.id)
                if (!target) return yield* unableToLoad(input.id)
                return yield* confirm(target).pipe(
                  Effect.mapError((error) => (error instanceof ToolFailure ? error : unableToLoad(input.id, error))),
                )
              }
              return yield* Effect.gen(function* () {
                yield* permission.assert({
                  action: name,
                  resources: [skill.id],
                  save: [skill.id],
                  ...Permission.invocation(context),
                })
                const { directory, output: content } = yield* Skill.prepare(
                  fs,
                  { ...skill, content: yield* skills.content(skill.id) },
                  yield* skills.files(skill.id),
                )
                return {
                  output: { directory, content },
                  content,
                  metadata: { name: skill.name, directory },
                }
              }).pipe(Effect.mapError((error) => unableToLoad(input.id, error)))
            }),
        }),
      )
      .pipe(Effect.orDie)
  }),
}
