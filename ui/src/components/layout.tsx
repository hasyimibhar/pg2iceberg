import { Link, Outlet } from "react-router-dom";
import { Database } from "lucide-react";

export function Layout() {
  return (
    <div className="min-h-screen bg-background">
      <header className="border-b">
        <div className="container mx-auto flex items-center px-6 py-4">
          <Link to="/" className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            <h1 className="text-lg font-semibold">pg2iceberg</h1>
          </Link>
        </div>
      </header>
      <main className="container mx-auto px-6 py-6">
        <Outlet />
      </main>
    </div>
  );
}
