import type { Route } from "./+types/home";
import { Welcome } from "../welcome/welcome";
import React, { useState, useEffect } from "react";

interface Team {
  id: number;
  franchiseId: number;
  fullName: string;
  triCode: string;
}

export function meta({}: Route.MetaArgs) {
  return [
    { title: "New React Router App" },
    { name: "description", content: "Welcome to React Router!" },
  ];
}

export default function Home() {
  const [teams, setTeams] = useState<Team[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchTeams = async () => {
      try {
        const response = await fetch("http://localhost:8000/teams");
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data: Team[] = await response.json();
        setTeams(data);
      } catch (e: any) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    };

    fetchTeams();
  }, []);

  if (loading) {
    return <div>Loading teams...</div>;
  }

  if (error) {
    return <div>Error: {error}</div>;
  }

  return (
    <>
      <Welcome />
      <div style={{ padding: "20px" }}>
        <h1>Select a Team</h1>
        <select style={{ width: "300px", height: "40px", fontSize: "16px" }}>
          <option value="">--Please choose an option--</option>
          {teams.map((team) => (
            <option key={team.id} value={team.id}>
              {team.fullName} ({team.triCode})
            </option>
          ))}
        </select>
      </div>
    </>
  );
}
