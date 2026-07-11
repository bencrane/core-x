import React from 'react';

function App() {
  return (
    <div className="min-h-screen bg-gov-cream text-gov-navy font-sans selection:bg-gov-navy selection:text-gov-cream">
      {/* Block 1: Navigation & Header */}
      <nav className="border-b-grid">
        <div className="max-w-[1440px] mx-auto px-6 grid grid-cols-1 lg:grid-cols-12 h-auto lg:h-24 items-center">
          <div className="col-span-1 lg:col-span-4 font-serif font-black text-2xl tracking-tighter uppercase py-6 lg:py-0 text-center lg:text-left border-b lg:border-b-0 border-gov-navy lg:border-transparent">
            Government Contracted
          </div>
          <div className="col-span-1 lg:col-span-5 flex flex-col lg:flex-row justify-center space-y-4 lg:space-y-0 lg:space-x-8 text-sm font-semibold tracking-widest uppercase py-6 lg:py-0 border-b lg:border-b-0 border-gov-navy lg:border-transparent text-center">
            <a href="#network" className="hover:text-gov-gold transition-colors">The Network</a>
            <a href="#infrastructure" className="hover:text-gov-gold transition-colors">Execution Infrastructure</a>
            <a href="#originators" className="hover:text-gov-gold transition-colors">Originators</a>
          </div>
          <div className="col-span-1 lg:col-span-3 flex justify-center lg:justify-end py-6 lg:py-0">
            <button className="bg-gov-navy text-gov-cream px-8 py-4 text-xs font-bold uppercase tracking-widest hover:bg-gov-gold hover:text-gov-navy transition-colors border border-gov-navy">
              Initiate Deployment Protocol
            </button>
          </div>
        </div>
      </nav>

      {/* Block 2: The Hero (The Catalyst) */}
      <header className="border-b-grid border-b-[2px]">
        <div className="max-w-[1440px] mx-auto">
          <div className="grid grid-cols-12 lg:border-x-grid border-gov-navy lg:mx-6">
            <div className="col-span-12 lg:col-span-8 p-8 md:p-12 lg:p-20 border-b-grid lg:border-b-0 lg:border-r-grid">
              <h1 className="font-serif font-black text-5xl md:text-6xl lg:text-[5.5rem] leading-[0.9] tracking-tight text-gov-navy uppercase">
                You Won the Award.<br />
                <span className="text-gov-gold block mt-2">Now You Have a Deficit.</span>
              </h1>
            </div>
            <div className="col-span-12 lg:col-span-4 p-8 md:p-12 lg:p-20 flex flex-col justify-end bg-white lg:bg-transparent">
              <p className="font-sans font-medium text-lg lg:text-xl leading-relaxed text-gov-navy border-l-4 border-gov-gold pl-6">
                <strong>Government Contracted</strong> is an event-driven origination desk. We engineer the capital, surety, and labor supply chains for federal prime contractors experiencing immediate, catalyst-driven execution demands.
              </p>
            </div>
          </div>
        </div>
      </header>

      {/* Block 3: The Mechanism (Dark Section) */}
      <section className="bg-gov-navy text-gov-cream border-b-grid">
        <div className="max-w-[1440px] mx-auto lg:border-x border-gov-cream lg:mx-6 border-opacity-20">
          <div className="grid grid-cols-1 lg:grid-cols-3">
            {/* Node 1 */}
            <div className="p-8 md:p-12 lg:p-16 border-b border-gov-cream border-opacity-20 lg:border-b-0 lg:border-r hover:bg-[#15213d] transition-colors">
              <div className="font-mono text-xs font-bold text-gov-cream opacity-50 mb-4 tracking-widest">NODE 01</div>
              <h3 className="font-serif font-bold text-gov-gold text-3xl uppercase tracking-tight mb-6">Execution<br/>Labor</h3>
              <p className="font-sans text-lg leading-relaxed text-gray-300 font-light">
                Precision routing of W-2 human capital mapped strictly to localized SCA/DBA statutory wage floors and socioeconomic pass-through requirements.
              </p>
            </div>
            
            {/* Node 2 */}
            <div className="p-8 md:p-12 lg:p-16 border-b border-gov-cream border-opacity-20 lg:border-b-0 lg:border-r hover:bg-[#15213d] transition-colors">
              <div className="font-mono text-xs font-bold text-gov-cream opacity-50 mb-4 tracking-widest">NODE 02</div>
              <h3 className="font-serif font-bold text-gov-gold text-3xl uppercase tracking-tight mb-6">Project<br/>Capital</h3>
              <p className="font-sans text-lg leading-relaxed text-gray-300 font-light">
                Mobilization financing, payroll float, and federal factor facilities structured specifically against your active SAM.gov award obligations.
              </p>
            </div>
            
            {/* Node 3 */}
            <div className="p-8 md:p-12 lg:p-16 hover:bg-[#15213d] transition-colors">
              <div className="font-mono text-xs font-bold text-gov-cream opacity-50 mb-4 tracking-widest">NODE 03</div>
              <h3 className="font-serif font-bold text-gov-gold text-3xl uppercase tracking-tight mb-6">Surety &<br/>Bonding</h3>
              <p className="font-sans text-lg leading-relaxed text-gray-300 font-light">
                Immediate routing for mandated bid, performance, and payment bonds matched to your exact NAICS profile and award ceiling.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* Block 4: The Authority / Footer (The Filter) */}
      <section className="bg-gov-cream">
        <div className="max-w-[1440px] mx-auto">
          <div className="lg:border-x-grid lg:mx-6">
            <div className="grid grid-cols-12 border-b-grid">
              <div className="col-span-12 lg:col-span-8 p-8 md:p-12 lg:p-20 border-b-grid lg:border-b-0 lg:border-r-grid">
                <h2 className="font-serif font-black text-4xl md:text-5xl lg:text-[4.5rem] leading-[1] uppercase tracking-tighter mb-8 text-gov-navy">
                  We Do Not Sell Software.<br />
                  <span className="text-gov-gold">We Deploy Infrastructure.</span>
                </h2>
                <p className="font-sans text-xl lg:text-2xl font-medium leading-relaxed max-w-3xl border-l-4 border-gov-navy pl-6">
                  We operate a closed network of specialized lenders, bonded sureties, and vetted labor originators. We only engage when a verified federal award triggers a structural deficit.
                </p>
              </div>
              <div className="col-span-12 lg:col-span-4 p-8 md:p-12 lg:p-20 flex flex-col justify-center bg-[#eaeaea] lg:bg-transparent">
                <div className="text-xs font-bold tracking-widest uppercase mb-4 text-gov-navy opacity-70">Intake Protocol Restrictions</div>
                <div className="font-serif text-3xl md:text-4xl font-black uppercase tracking-tight leading-tight">Active Award Holders Only</div>
              </div>
            </div>
            
            {/* Action Block */}
            <div className="p-6 md:p-12 lg:p-24 flex flex-col items-center justify-center min-h-[50vh] bg-gov-cream relative overflow-hidden">
              {/* Grid background effect */}
              <div className="absolute inset-0 opacity-[0.03]" style={{backgroundImage: 'linear-gradient(var(--color-gov-navy) 1px, transparent 1px), linear-gradient(90deg, var(--color-gov-navy) 1px, transparent 1px)', backgroundSize: '40px 40px'}}></div>
              
              <div className="w-full max-w-3xl border-2 border-gov-navy p-8 md:p-12 lg:p-16 bg-white relative z-10 shadow-[8px_8px_0px_0px_rgba(15,23,42,1)]">
                {/* Decorative brutalist corner markers */}
                <div className="absolute top-2 left-2 w-4 h-4 border-t-4 border-l-4 border-gov-navy"></div>
                <div className="absolute top-2 right-2 w-4 h-4 border-t-4 border-r-4 border-gov-navy"></div>
                <div className="absolute bottom-2 left-2 w-4 h-4 border-b-4 border-l-4 border-gov-navy"></div>
                <div className="absolute bottom-2 right-2 w-4 h-4 border-b-4 border-r-4 border-gov-navy"></div>
                
                <h3 className="font-serif font-black text-3xl md:text-4xl uppercase tracking-tighter text-center mb-2">Submit Task Order Deficit</h3>
                <p className="text-center text-sm font-semibold uppercase tracking-widest mb-10 text-gov-navy opacity-60">System Ready for Award Verification</p>
                
                <form className="space-y-8" onSubmit={(e) => e.preventDefault()}>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div className="space-y-3">
                      <label className="block text-xs font-bold uppercase tracking-widest">SAM.gov Award ID (PIID)</label>
                      <input type="text" className="w-full border-b-2 border-gov-navy bg-transparent pb-3 font-mono text-base focus:outline-none focus:border-gov-gold placeholder-gray-300 transition-colors rounded-none" placeholder="e.g. W9128F21C0012" />
                    </div>
                    <div className="space-y-3">
                      <label className="block text-xs font-bold uppercase tracking-widest">UEI (Unique Entity ID)</label>
                      <input type="text" className="w-full border-b-2 border-gov-navy bg-transparent pb-3 font-mono text-base focus:outline-none focus:border-gov-gold placeholder-gray-300 transition-colors rounded-none" placeholder="12-character alphanumeric" />
                    </div>
                  </div>
                  
                  <div className="space-y-3">
                    <label className="block text-xs font-bold uppercase tracking-widest">Primary Deficit Category</label>
                    <select className="w-full border-2 border-gov-navy bg-transparent p-4 font-mono text-sm font-bold focus:outline-none focus:ring-2 focus:ring-gov-gold appearance-none rounded-none cursor-pointer">
                      <option>-- SELECT CATEGORY --</option>
                      <option>PROJECT CAPITAL (MOBILIZATION/FLOAT)</option>
                      <option>SURETY & BONDING</option>
                      <option>EXECUTION LABOR (SCA/DBA)</option>
                    </select>
                  </div>
                  
                  <button type="button" className="w-full bg-gov-navy text-gov-cream font-black text-lg uppercase tracking-widest py-6 hover:bg-gov-gold hover:text-gov-navy transition-all border-2 border-gov-navy mt-6 active:translate-y-1 active:shadow-none shadow-[4px_4px_0px_0px_rgba(184,156,106,1)]">
                    Verify Award & Initialize
                  </button>
                </form>
              </div>
            </div>
            
            {/* Footer Base */}
            <div className="border-t-grid p-8 grid grid-cols-1 md:grid-cols-3 gap-4 items-center bg-gov-navy text-gov-cream">
              <div className="text-center md:text-left text-xs font-bold uppercase tracking-widest opacity-80">
                &copy; {new Date().getFullYear()} Government Contracted
              </div>
              <div className="text-center text-xs font-mono tracking-wider text-gov-gold flex justify-center items-center">
                <span className="w-2 h-2 rounded-full bg-green-500 mr-2 animate-pulse"></span>
                SYSTEMS OPERATIONAL
              </div>
              <div className="text-center md:text-right text-xs font-bold uppercase tracking-widest opacity-80">
                Authorized Access Only
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

export default App;
