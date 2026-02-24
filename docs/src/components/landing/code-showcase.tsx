"use client";

import { motion } from "framer-motion";
import { CodeBlock } from "./code-block";
import { SectionHeader } from "./section-header";

const pgCode = `package main

import (
  "context"
  "fmt"

  "github.com/xraph/grove"
  "github.com/xraph/grove/drivers/pgdriver"
)

type User struct {
  grove.BaseModel \`grove:"table:users,alias:u"\`

  ID        int64     \`grove:"id,pk,autoincrement"\`
  Name      string    \`grove:"name,notnull"\`
  Email     string    \`grove:"email,notnull,unique"\`
  Role      string    \`grove:"role,default:'user'"\`
  Metadata  JSONMap   \`grove:"metadata,type:jsonb"\`
}

func main() {
  ctx := context.Background()

  // Create and open the driver
  pgdb := pgdriver.New()
  pgdb.Open(ctx, "postgres://user:pass@localhost/mydb")

  // Pass connected driver to Grove
  db, _ := grove.Open(pgdb)
  defer db.Close()

  // Use typed driver for queries
  pg := pgdriver.Unwrap(db)
  var users []User
  _ = pg.NewSelect(&users).
    Where("email ILIKE $1", "%@example.com").
    Where("metadata->>'tier' = $2", "premium").
    DistinctOn("email").
    OrderExpr("email, created_at DESC").
    Limit(50).
    Scan(ctx)
  fmt.Printf("found %d users\\n", len(users))
}`;

const mongoCode = `package main

import (
  "context"
  "fmt"

  "github.com/xraph/grove"
  "github.com/xraph/grove/drivers/mongodriver"
  "go.mongodb.org/mongo-driver/bson"
)

type User struct {
  grove.BaseModel \`grove:"table:users"\`

  ID    string \`grove:"_id,pk"\`
  Name  string \`grove:"name,notnull"\`
  Email string \`grove:"email,notnull,unique"\`
  Role  string \`grove:"role"\`
}

func main() {
  ctx := context.Background()

  // Create and open the driver
  mgdrv := mongodriver.New()
  mgdrv.Open(ctx, "mongodb://localhost:27017/mydb")

  // Pass connected driver to Grove
  db, _ := grove.Open(mgdrv)
  defer db.Close()

  mgdb := mongodriver.Unwrap(db)
  var users []User
  _ = mgdb.NewFind(&users).
    Filter(bson.M{
      "email": bson.M{"$regex": "@example.com$"},
      "role":  bson.M{"$in": bson.A{"admin", "mod"}},
    }).
    Sort(bson.D{{"email", 1}, {"created_at", -1}}).
    Limit(50).
    Scan(ctx)
  fmt.Printf("found %d users\\n", len(users))
}`;

export function CodeShowcase() {
  return (
    <section className="relative w-full py-20 sm:py-28">
      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <SectionHeader
          badge="Developer Experience"
          title="Native syntax. Maximum performance."
          description="Write PostgreSQL queries that read like PostgreSQL and MongoDB queries that read like MongoDB. Grove respects each database's native idioms."
        />

        <div className="mt-14 grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* PostgreSQL side */}
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.1 }}
          >
            <div className="mb-3 flex items-center gap-2">
              <div className="size-2 rounded-full bg-blue-500" />
              <span className="text-xs font-medium text-fd-muted-foreground uppercase tracking-wider">
                PostgreSQL
              </span>
            </div>
            <CodeBlock code={pgCode} filename="postgres.go" />
          </motion.div>

          {/* MongoDB side */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
          >
            <div className="mb-3 flex items-center gap-2">
              <div className="size-2 rounded-full bg-indigo-500" />
              <span className="text-xs font-medium text-fd-muted-foreground uppercase tracking-wider">
                MongoDB
              </span>
            </div>
            <CodeBlock code={mongoCode} filename="mongo.go" />
          </motion.div>
        </div>
      </div>
    </section>
  );
}
