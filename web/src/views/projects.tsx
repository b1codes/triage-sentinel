import { useQuery } from '@tanstack/react-query'
import { getProjects } from '../lib/api'

export function ProjectsView() {
  const query = useQuery({ queryKey: ['projects'], queryFn: getProjects })

  if (query.isPending) return <p className="sub">Loading…</p>
  if (query.isError) return <p className="error">{(query.error as Error).message}</p>

  return (
    <section>
      <h2>Projects</h2>
      <table className="grid">
        <thead>
          <tr>
            <th scope="col">Slug</th>
            <th scope="col">Repository</th>
            <th scope="col">Branch</th>
            <th scope="col">Incidents</th>
            <th scope="col">Status</th>
          </tr>
        </thead>
        <tbody>
          {query.data.projects.map((project) => (
            <tr key={project.slug} className={project.active ? undefined : 'inactive'}>
              <td>{project.slug}</td>
              <td>{project.repo}</td>
              <td>{project.default_branch}</td>
              <td>{project.incidents}</td>
              <td>
                {project.quarantined ? (
                  <span className="bad" title={project.quarantine_reason}>
                    quarantined
                  </span>
                ) : project.active ? (
                  <span className="ok">active</span>
                ) : (
                  <span className="sub">deregistered</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="sub">
        A deregistered project keeps its incident history; removing it from{' '}
        <code>projects.yaml</code> never deletes rows.
      </p>
    </section>
  )
}
