import React from 'react';

function App() {
  return (
    <div className="min-h-screen bg-[#fafafa] text-slate-900 font-sans selection:bg-slate-900 selection:text-white">
      {/* Navigation */}
      <nav className="sticky top-0 z-50 bg-white/80 backdrop-blur-md border-b border-slate-200">
        <div className="max-w-7xl mx-auto px-6 h-20 flex items-center justify-between">
          <div className="font-serif font-semibold text-xl tracking-tight text-slate-900">
            Government Contracted
          </div>
          <div className="hidden md:flex space-x-8 text-sm font-medium text-slate-600">
            <a href="#network" className="hover:text-slate-900 transition-colors">The Network</a>
            <a href="#infrastructure" className="hover:text-slate-900 transition-colors">Infrastructure</a>
            <a href="#originators" className="hover:text-slate-900 transition-colors">Originators</a>
          </div>
          <div>
            <button className="bg-slate-900 text-white px-5 py-2.5 rounded-lg text-sm font-medium hover:bg-slate-800 transition-all shadow-sm">
              Initiate Deployment
            </button>
          </div>
        </div>
      </nav>

      {/* Hero Section */}
      <header className="relative pt-24 pb-32 overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-b from-slate-50 to-white -z-10"></div>
        <div className="max-w-5xl mx-auto px-6 text-center">
          <div className="inline-flex items-center px-3 py-1 rounded-full bg-slate-100 text-slate-700 text-xs font-medium mb-8 border border-slate-200">
            <span className="w-2 h-2 rounded-full bg-amber-500 mr-2"></span>
            Event-Driven Origination Desk
          </div>
          <h1 className="font-serif font-medium text-5xl md:text-7xl leading-tight tracking-tight text-slate-900 mb-6 text-balance mx-auto">
            You won the award.<br />
            <span className="text-slate-500">Now you have a deficit.</span>
          </h1>
          <p className="font-sans text-lg md:text-xl text-slate-600 max-w-3xl mx-auto leading-relaxed mb-10 text-balance">
            We engineer the capital, surety, and labor supply chains for federal prime contractors experiencing immediate, catalyst-driven execution demands.
          </p>
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <button className="w-full sm:w-auto bg-slate-900 text-white px-8 py-3.5 rounded-lg font-medium hover:bg-slate-800 transition-all shadow-md hover:shadow-lg">
              Submit Task Order Deficit
            </button>
            <button className="w-full sm:w-auto bg-white text-slate-700 px-8 py-3.5 rounded-lg font-medium border border-slate-200 hover:bg-slate-50 hover:text-slate-900 transition-all shadow-sm">
              Explore Infrastructure
            </button>
          </div>
        </div>
      </header>

      {/* Mechanism Section */}
      <section className="py-24 bg-slate-900 text-white relative">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16">
            <h2 className="font-serif text-3xl md:text-4xl font-medium mb-4">Fulfillment Infrastructure</h2>
            <p className="text-slate-400 max-w-2xl mx-auto">We operate a closed network of specialized lenders, bonded sureties, and vetted labor originators.</p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            {/* Node 1 */}
            <div className="bg-slate-800/50 border border-slate-700/50 rounded-2xl p-8 backdrop-blur-sm hover:bg-slate-800 transition-colors">
              <div className="font-mono text-xs text-amber-500 mb-6 tracking-wider uppercase font-semibold">Node 01</div>
              <h3 className="font-serif text-2xl mb-4">Execution Labor</h3>
              <p className="text-slate-400 leading-relaxed">
                Precision routing of W-2 human capital mapped strictly to localized SCA/DBA statutory wage floors and socioeconomic pass-through requirements.
              </p>
            </div>
            
            {/* Node 2 */}
            <div className="bg-slate-800/50 border border-slate-700/50 rounded-2xl p-8 backdrop-blur-sm hover:bg-slate-800 transition-colors">
              <div className="font-mono text-xs text-amber-500 mb-6 tracking-wider uppercase font-semibold">Node 02</div>
              <h3 className="font-serif text-2xl mb-4">Project Capital</h3>
              <p className="text-slate-400 leading-relaxed">
                Mobilization financing, payroll float, and federal factor facilities structured specifically against your active SAM.gov award obligations.
              </p>
            </div>
            
            {/* Node 3 */}
            <div className="bg-slate-800/50 border border-slate-700/50 rounded-2xl p-8 backdrop-blur-sm hover:bg-slate-800 transition-colors">
              <div className="font-mono text-xs text-amber-500 mb-6 tracking-wider uppercase font-semibold">Node 03</div>
              <h3 className="font-serif text-2xl mb-4">Surety & Bonding</h3>
              <p className="text-slate-400 leading-relaxed">
                Immediate routing for mandated bid, performance, and payment bonds matched to your exact NAICS profile and award ceiling.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* Form / Intake Section */}
      <section className="py-24 bg-[#fafafa]">
        <div className="max-w-4xl mx-auto px-6">
          <div className="bg-white rounded-3xl p-8 md:p-12 shadow-sm border border-slate-200/60 relative overflow-hidden">
            <div className="max-w-2xl relative z-10">
              <h2 className="font-serif text-3xl font-medium mb-2">Initialize Deployment</h2>
              <p className="text-slate-500 mb-8">We only engage when a verified federal award triggers a structural deficit.</p>
              
              <form className="space-y-6" onSubmit={(e) => e.preventDefault()}>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div className="space-y-2">
                    <label className="block text-sm font-medium text-slate-700">SAM.gov Award ID (PIID)</label>
                    <input 
                      type="text" 
                      className="w-full border border-slate-300 rounded-lg bg-white px-4 py-3 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-slate-400 focus:border-slate-400 transition-all placeholder:text-slate-400" 
                      placeholder="e.g. W9128F21C0012" 
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="block text-sm font-medium text-slate-700">UEI (Unique Entity ID)</label>
                    <input 
                      type="text" 
                      className="w-full border border-slate-300 rounded-lg bg-white px-4 py-3 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-slate-400 focus:border-slate-400 transition-all placeholder:text-slate-400" 
                      placeholder="12-character alphanumeric" 
                    />
                  </div>
                </div>
                
                <div className="space-y-2">
                  <label className="block text-sm font-medium text-slate-700">Primary Deficit Category</label>
                  <select className="w-full border border-slate-300 rounded-lg bg-white px-4 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-slate-400 focus:border-slate-400 transition-all">
                    <option value="">Select category...</option>
                    <option>Project Capital (Mobilization/Float)</option>
                    <option>Surety & Bonding</option>
                    <option>Execution Labor (SCA/DBA)</option>
                  </select>
                </div>
                
                <button type="button" className="bg-slate-900 text-white font-medium rounded-lg px-6 py-3.5 hover:bg-slate-800 transition-all w-full shadow-sm mt-4">
                  Verify Award & Initialize
                </button>
              </form>
            </div>
          </div>
        </div>
      </section>
      
      {/* Footer */}
      <footer className="border-t border-slate-200 bg-white py-12">
        <div className="max-w-7xl mx-auto px-6 flex flex-col md:flex-row justify-between items-center gap-4 text-sm text-slate-500">
          <div>
            &copy; {new Date().getFullYear()} Government Contracted. All rights reserved.
          </div>
          <div className="flex items-center font-mono text-xs text-slate-400">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 mr-2"></span>
            SYSTEMS OPERATIONAL
          </div>
        </div>
      </footer>
    </div>
  );
}

export default App;
