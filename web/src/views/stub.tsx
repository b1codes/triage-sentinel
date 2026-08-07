export function ComingSoon({ view, milestone }: { view: string; milestone: string }) {
  return (
    <section className="empty">
      <h2>{view}</h2>
      <p className="sub">
        {milestone ? `This view arrives in ${milestone}.` : 'Nothing here.'}
      </p>
    </section>
  )
}
