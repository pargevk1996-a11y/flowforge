/** A payload, pretty-printed. Debugging means reading the actual bytes. */
export function Json({ value, label }: { value: unknown; label?: string }) {
  if (value === null || value === undefined) return null;
  return (
    <div className="json">
      {label !== undefined && <div className="json-label">{label}</div>}
      <pre>{JSON.stringify(value, null, 2)}</pre>
    </div>
  );
}
