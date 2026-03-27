export async function createSqliteRuntime({
  dbPath,
  wasmUrl,
  createSqliteModule,
  sqliteApi,
  createVfs,
}) {
  const module = await createSqliteModule({
    locateFile: (file) => file.endsWith('.wasm') ? wasmUrl : file,
  });
  const sqlite3 = sqliteApi.Factory(module);
  const vfs = await createVfs(module);
  sqlite3.vfs_register(vfs, true);
  const db = await sqlite3.open_v2(dbPath);

  return {
    sqlite3,
    db,
    async exec(sql) {
      await sqlite3.exec(db, sql);
    },
    async insertBatch(sql, paramSets) {
      for await (const stmt of sqlite3.statements(db, sql)) {
        for (const params of paramSets) {
          await sqlite3.reset(stmt);
          sqlite3.bind_collection(stmt, params);
          await sqlite3.step(stmt);
        }
      }
    },
    async close() {
      try {
        await sqlite3.close(db);
      } finally {
        vfs.close?.();
      }
    },
  };
}
