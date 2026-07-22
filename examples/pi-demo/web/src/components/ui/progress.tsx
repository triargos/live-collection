import { Progress as ProgressPrimitive } from "radix-ui"
import * as React from "react"
import { cn } from "@/lib/utils"

function Progress({ className, value, ...props }: React.ComponentProps<typeof ProgressPrimitive.Root>) {
  const normalizedValue = Math.max(0, Math.min(100, value ?? 0))
  return (
    <ProgressPrimitive.Root
      data-slot="progress"
      className={cn("relative h-3 w-full overflow-hidden rounded-full bg-secondary ring-1 ring-primary/10", className)}
      value={normalizedValue}
      {...props}
    >
      <ProgressPrimitive.Indicator
        data-slot="progress-indicator"
        className="h-full w-full rounded-full bg-gradient-to-r from-primary to-fuchsia-400 transition-transform duration-500 ease-out"
        style={{ transform: `translateX(-${100 - normalizedValue}%)` }}
      />
    </ProgressPrimitive.Root>
  )
}

export { Progress }
